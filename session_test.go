package yamux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logCapture struct{ bytes.Buffer }

func (l *logCapture) logs() []string {
	lines := strings.Split(strings.TrimSpace(l.String()), "\n")
	for i, line := range lines {
		// trim leading date.
		split := strings.SplitN(line, " ", 3)
		lines[i] = split[2]
	}
	return lines
}

func (l *logCapture) match(expect []string) bool {
	return reflect.DeepEqual(l.logs(), expect)
}

type pipeConn struct {
	net.Conn
	writeDeadline pipeDeadline
	writeBlocker  chan struct{}
	closeCh       chan struct{}
	closeOnce     sync.Once
	closeErr      error
}

func (p *pipeConn) SetDeadline(t time.Time) error {
	p.writeDeadline.set(t)
	return p.Conn.SetDeadline(t)
}

func (p *pipeConn) SetWriteDeadline(t time.Time) error {
	p.writeDeadline.set(t)
	return p.Conn.SetWriteDeadline(t)
}

func (p *pipeConn) Write(b []byte) (int, error) {
	select {
	case p.writeBlocker <- struct{}{}:
	case <-p.writeDeadline.wait():
		return 0, ErrTimeout
	case <-p.closeCh:
		return 0, io.ErrClosedPipe
	}
	n, err := p.Conn.Write(b)
	<-p.writeBlocker
	return n, err
}

func (p *pipeConn) Close() error {
	p.closeOnce.Do(func() {
		p.writeDeadline.set(time.Time{})
		p.closeErr = p.Conn.Close()
		close(p.closeCh)
	})
	return p.closeErr
}

func (p *pipeConn) BlockWrites() {
	p.writeBlocker <- struct{}{}
}

func (p *pipeConn) UnblockWrites() {
	<-p.writeBlocker
}

func testConn() (conn1, conn2 net.Conn) {
	c1, c2 := net.Pipe()
	conn1 = &pipeConn{
		Conn:          c1,
		writeDeadline: makePipeDeadline(),
		writeBlocker:  make(chan struct{}, 1),
		closeCh:       make(chan struct{}, 1),
	}
	conn2 = &pipeConn{
		Conn:          c2,
		writeDeadline: makePipeDeadline(),
		writeBlocker:  make(chan struct{}, 1),
		closeCh:       make(chan struct{}, 1),
	}
	return conn1, conn2
}

func testConf() *Config {
	conf := DefaultConfig()
	conf.AcceptBacklog = 64
	conf.KeepAliveInterval = 100 * time.Millisecond
	conf.ConnectionWriteTimeout = 350 * time.Millisecond
	return conf
}

func testConfNoKeepAlive() *Config {
	conf := testConf()
	conf.EnableKeepAlive = false
	return conf
}

func testClientServer() (*Session, *Session) {
	return testClientServerConfig(testConf())
}

func testClientServerConfig(conf *Config) (*Session, *Session) {
	conn1, conn2 := testConn()
	client, _ := Client(conn1, conf, nil)
	server, _ := Server(conn2, conf, nil)
	return client, server
}

func TestClientClient(t *testing.T) {
	conf := testConf()
	conn1, conn2 := testConn()
	client1, _ := Client(conn1, conf, nil)
	client2, _ := Client(conn2, conf, nil)
	defer client1.Close()
	defer client2.Close()

	_, _ = client1.OpenStream(context.Background())
	_, err := client2.AcceptStream()
	if err == nil {
		t.Fatalf("should have failed to open a stream with two clients")
	}
	_, _ = client2.OpenStream(context.Background())
	_, err = client1.AcceptStream()
	if err == nil {
		t.Fatalf("should have failed to open a stream with two clients")
	}
	if !client1.IsClosed() || !client2.IsClosed() {
		t.Fatalf("sessions should have been closed by errors")
	}
}

func TestServerServer(t *testing.T) {
	conf := testConf()
	conn1, conn2 := testConn()
	server1, _ := Server(conn1, conf, nil)
	server2, _ := Server(conn2, conf, nil)
	defer server1.Close()
	defer server2.Close()

	_, _ = server1.OpenStream(context.Background())
	_, err := server2.AcceptStream()
	if err == nil {
		t.Fatalf("should have failed to open a stream with two servers")
	}
	_, _ = server2.OpenStream(context.Background())
	_, err = server1.AcceptStream()
	if err == nil {
		t.Fatalf("should have failed to open a stream with two servers")
	}
	if !server1.IsClosed() || !server2.IsClosed() {
		t.Fatalf("sessions should have been closed by errors")
	}
}

func TestOpenStreamTimeout(t *testing.T) {
	const limit = 7
	cfg := testConf()
	cfg.AcceptBacklog = limit
	client, _ := testClientServerConfig(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < limit; i++ {
		if _, err := client.OpenStream(ctx); err != nil {
			t.Fatalf("didn't expect OpenStream to fail %s", err.Error())
		}
	}
	if _, err := client.OpenStream(ctx); err != context.DeadlineExceeded {
		t.Fatal("expected OpenStream to run into the deadline")
	}
}

func TestStreamAfterShutdown(t *testing.T) {
	do := func(cb func(s *Stream)) {
		var wg sync.WaitGroup
		client, server := testClientServer()
		wg.Add(2)

		go func() {
			defer wg.Done()
			s, err := client.OpenStream(context.Background())
			if err == nil {
				cb(s)
				_ = s.Reset()
			}
			client.Close()
		}()
		go func() {
			defer wg.Done()
			server.Close()
		}()
		wg.Wait()
	}
	// test reset
	for i := 0; i < 100; i++ {
		do(func(s *Stream) {})
	}
	// test close
	for i := 0; i < 100; i++ {
		do(func(s *Stream) {
			s.Close()
		})
	}

	// test write
	for i := 0; i < 100; i++ {
		do(func(s *Stream) {
			_, _ = s.Write([]byte{10})
		})
	}

	// test read
	for i := 0; i < 100; i++ {
		do(func(s *Stream) {
			_, _ = s.Read([]byte{10})
		})
	}
}

func TestPing(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	clientConn := client.conn.(*pipeConn)
	time.Sleep(time.Millisecond)

	clientConn.BlockWrites()
	go func() {
		time.Sleep(10 * time.Millisecond)
		clientConn.UnblockWrites()
	}()

	rtt, err := client.Ping()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if rtt == 0 {
		t.Fatalf("bad: %v", rtt)
	}

	clientConn.BlockWrites()
	go func() {
		time.Sleep(time.Millisecond)
		clientConn.UnblockWrites()
	}()

	rtt, err = server.Ping()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if rtt == 0 {
		t.Fatalf("bad: %v", rtt)
	}
}

func TestCloseBeforeAck(t *testing.T) {
	cfg := testConf()
	cfg.AcceptBacklog = 8
	client, server := testClientServerConfig(cfg)

	defer client.Close()
	defer server.Close()

	for i := 0; i < 8; i++ {
		s, err := client.OpenStream(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	for i := 0; i < 8; i++ {
		s, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s, err := client.OpenStream(context.Background())
		if err != nil {
			t.Error(err)
			return
		}
		s.Close()
	}()

	select {
	case <-done:
	case <-time.After(time.Second * 5):
		client.Close()
		server.Close()
		<-done
		t.Fatal("timed out trying to open stream")
	}
}

func TestAccept(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	if client.NumStreams() != 0 {
		t.Fatalf("bad")
	}
	if server.NumStreams() != 0 {
		t.Fatalf("bad")
	}

	wg := &sync.WaitGroup{}
	wg.Add(4)

	go func() {
		defer wg.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if id := stream.StreamID(); id != 1 {
			t.Errorf("bad: %v", id)
			return
		}
		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		stream, err := client.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if id := stream.StreamID(); id != 2 {
			t.Errorf("bad: %v", id)
			return
		}
		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		stream, err := server.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if id := stream.StreamID(); id != 2 {
			t.Errorf("bad: %v", id)
			return
		}
		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		stream, err := client.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if id := stream.StreamID(); id != 1 {
			t.Errorf("bad: %v", id)
			return
		}
		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		server.Close()
		client.Close()
		wg.Wait()
		t.Fatal("timeout")
	}
}

func TestNonNilInterface(t *testing.T) {
	_, server := testClientServer()
	server.Close()

	conn, err := server.Accept()
	if err != nil && conn != nil {
		t.Error("bad: accept should return a connection of nil value")
	}

	conn, err = server.Open(context.Background())
	if err != nil && conn != nil {
		t.Error("bad: open should return a connection of nil value")
	}
}

func TestSendData_Small(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		if server.NumStreams() != 1 {
			t.Errorf("bad")
			return
		}

		buf := make([]byte, 4)
		for i := 0; i < 1000; i++ {
			n, err := stream.Read(buf)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("short read: %d", n)
				return
			}
			if string(buf) != "test" {
				t.Errorf("bad: %s", buf)
				return
			}
		}

		if err := stream.CloseWrite(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
		n, err := stream.Read([]byte{0})
		if n != 0 || err != io.EOF {
			t.Errorf("err: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		stream, err := client.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		if client.NumStreams() != 1 {
			t.Errorf("bad")
			return
		}

		for i := 0; i < 1000; i++ {
			n, err := stream.Write([]byte("test"))
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("short write %d", n)
				return
			}
		}

		if err := stream.CloseWrite(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
		n, err := stream.Read([]byte{0})
		if n != 0 || err != io.EOF {
			t.Errorf("err: %v", err)
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		client.Close()
		server.Close()
		wg.Wait()
		t.Fatal("timeout")
	}

	if client.NumStreams() != 0 {
		t.Fatalf("bad")
	}
	if server.NumStreams() != 0 {
		t.Fatalf("bad")
	}
}

func TestSendData_Large(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	const (
		sendSize = 250 * 1024 * 1024
		recvSize = 4 * 1024
	)

	data := make([]byte, sendSize)
	for idx := range data {
		data[idx] = byte(idx % 256)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		var sz int
		buf := make([]byte, recvSize)
		for i := 0; i < sendSize/recvSize; i++ {
			n, err := io.ReadFull(stream, buf)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != recvSize {
				t.Errorf("short read: %d", n)
				return
			}
			sz += n
			for idx := range buf {
				if buf[idx] != byte(idx%256) {
					t.Errorf("bad: %v %v %v", i, idx, buf[idx])
					return
				}
			}
		}

		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		stream, err := client.Open(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		n, err := stream.Write(data)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if n != len(data) {
			t.Errorf("short write %d", n)
			return
		}

		if err := stream.Close(); err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(20 * time.Second):
		client.Close()
		server.Close()
		wg.Wait()
		t.Fatal("timeout")
	}
}

func TestGoAway(t *testing.T) {
	// This test is noisy.
	conf := testConf()
	conf.LogOutput = io.Discard

	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	if err := server.GoAway(); err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 100; i++ {
		s, err := client.Open(context.Background())
		if err == nil {
			s.Close()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != ErrRemoteGoAway {
			t.Fatalf("expected %s, got %s", ErrRemoteGoAway, err)
		} else {
			return
		}
	}
	t.Fatalf("expected GoAway error")
}

func TestCloseWithError(t *testing.T) {
	// This test is noisy.
	conf := testConf()
	conf.LogOutput = io.Discard

	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	if err := server.CloseWithError(42); err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 100; i++ {
		s, err := client.Open(context.Background())
		if err == nil {
			s.Close()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if !errors.Is(err, &GoAwayError{ErrorCode: 42, Remote: true}) {
			t.Fatalf("err: %v", err)
		}
		return
	}
	t.Fatalf("expected GoAway error")
}

func TestManyStreams(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wg := &sync.WaitGroup{}

	acceptor := func(i int) {
		defer wg.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		buf := make([]byte, 512)
		for {
			n, err := stream.Read(buf)
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n == 0 {
				t.Errorf("err: %v", err)
				return
			}
		}
	}
	sender := func(i int) {
		defer wg.Done()
		stream, err := client.Open(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		msg := fmt.Sprintf("%08d", i)
		for i := 0; i < 1000; i++ {
			n, err := stream.Write([]byte(msg))
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != len(msg) {
				t.Errorf("short write %d", n)
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go acceptor(i)
		go sender(i)
	}

	wg.Wait()
}

func TestManyStreams_PingPong(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wg := &sync.WaitGroup{}

	ping := []byte("ping")
	pong := []byte("pong")

	acceptor := func(i int) {
		defer wg.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		buf := make([]byte, 4)
		for {
			// Read the 'ping'
			n, err := io.ReadFull(stream, buf)
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("err: %v", err)
				return
			}
			if !bytes.Equal(buf, ping) {
				t.Errorf("bad: %s", buf)
				return
			}

			// Write out the 'pong'
			n, err = stream.Write(pong)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("err: %v", err)
				return
			}
		}
	}
	sender := func(i int) {
		defer wg.Done()
		stream, err := client.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		buf := make([]byte, 4)
		for i := 0; i < 1000; i++ {
			// Send the 'ping'
			n, err := stream.Write(ping)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("short write %d", n)
				return
			}

			// Read the 'pong'
			n, err = io.ReadFull(stream, buf)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if n != 4 {
				t.Errorf("err: %v", err)
				return
			}
			if !bytes.Equal(buf, pong) {
				t.Errorf("bad: %s", buf)
				return
			}
		}
	}

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go acceptor(i)
		go sender(i)
	}

	wg.Wait()
}

func TestCloseRead(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err = stream.Write([]byte("a")); err != nil {
		t.Fatalf("err: %v", err)
	}

	stream2, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	stream2.CloseRead()

	buf := make([]byte, 4)
	n, err := stream2.Read(buf)
	if n != 0 || err == nil {
		t.Fatalf("read after close: %d %s", n, err)
	}
}

func TestHalfClose(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err = stream.Write([]byte("a")); err != nil {
		t.Fatalf("err: %v", err)
	}

	stream2, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	stream2.CloseWrite() // Half close

	buf := make([]byte, 4)
	n, err := io.ReadAtLeast(stream2, buf, 1)
	if err != nil && err != io.EOF {
		t.Fatalf("err: %v", err)
	}
	if n != 1 {
		t.Fatalf("bad: %v", n)
	}

	// Send more
	if _, err = stream.Write([]byte("bcd")); err != nil {
		t.Fatalf("err: %v", err)
	}
	stream.CloseWrite()

	// write after close
	n, err = stream.Write([]byte("foobar"))
	if n != 0 || err == nil {
		t.Fatalf("wrote after close: %d %s", n, err)
	}

	// Read after close
	n, err = io.ReadAtLeast(stream2, buf, 3)
	if err != nil && err != io.EOF {
		t.Fatalf("err: %v", err)
	}
	if n != 3 {
		t.Fatalf("bad: %v", n)
	}

	// EOF after close
	n, err = stream2.Read(buf)
	if err != io.EOF {
		t.Fatalf("err: %v", err)
	}
	if n != 0 {
		t.Fatalf("bad: %v", n)
	}
}

func TestReadDeadline(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	stream, err := client.Open(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream2.Close()

	if err := stream.SetReadDeadline(time.Now().Add(5 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	buf := make([]byte, 4)
	if _, err := stream.Read(buf); err != ErrTimeout {
		t.Fatalf("err: %v", err)
	}
}

func TestWriteDeadlineWindowFull(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	stream, err := client.Open(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream2.Close()

	if err := stream.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	buf := make([]byte, 512)
	for i := 0; i < int(initialStreamWindow); i++ {
		_, err := stream.Write(buf)
		if err != nil && err == ErrTimeout {
			return
		} else if err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	t.Fatalf("Expected timeout")
}

func TestBacklogExceeded(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	// Fill the backlog
	max := client.config.AcceptBacklog
	for i := 0; i < max; i++ {
		stream, err := client.Open(context.Background())
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer stream.Close()

		if _, err := stream.Write([]byte("foo")); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Attempt to open a new stream
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Open(context.Background())
		errCh <- err
	}()

	// Shutdown the server
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.Close()
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("open should fail")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
}

func TestKeepAlive(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	// Ping value should increase
	require.Eventually(t, func() bool {
		client.pingLock.Lock()
		defer client.pingLock.Unlock()
		return client.pingID > 0
	}, time.Second, 50*time.Millisecond, "should ping")

	require.Eventually(t, func() bool {
		server.pingLock.Lock()
		defer server.pingLock.Unlock()
		return server.pingID > 0
	}, time.Second, 50*time.Millisecond, "should ping")
}

func TestKeepAlive_Timeout(t *testing.T) {
	conn1, conn2 := testConn()

	clientConf := testConf()
	clientConf.ConnectionWriteTimeout = time.Hour // We're testing keep alives, not connection writes
	clientConf.EnableKeepAlive = false            // Just test one direction, so it's deterministic who hangs up on whom
	client, _ := Client(conn1, clientConf, nil)
	defer client.Close()

	serverLogs := new(logCapture)
	serverConf := testConf()
	serverConf.LogOutput = serverLogs

	server, _ := Server(conn2, serverConf, nil)
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := server.Accept() // Wait until server closes
		errCh <- err
	}()

	// Prevent the client from responding
	clientConn := client.conn.(*pipeConn)
	clientConn.BlockWrites()
	defer clientConn.UnblockWrites()

	select {
	case err := <-errCh:
		if err != ErrKeepAliveTimeout {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for timeout")
	}

	if !server.IsClosed() {
		t.Fatalf("server should have closed")
	}

	if !serverLogs.match([]string{"[ERR] yamux: keepalive failed: i/o deadline reached"}) {
		t.Fatalf("server log incorect: %v", serverLogs.logs())
	}
}

type UnlimitedReader struct{}

func (u *UnlimitedReader) Read(p []byte) (int, error) {
	runtime.Gosched()
	return len(p), nil
}

func TestBacklogExceeded_Accept(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()

	max := 5 * client.config.AcceptBacklog
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		for i := 0; i < max; i++ {
			stream, err := server.Accept()
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			defer stream.Close()
		}
	}()

	// Fill the backlog
	for i := 0; i < max; i++ {
		stream, err := client.Open(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
		}
		defer stream.Close()

		if _, err := stream.Write([]byte("foo")); err != nil {
			t.Errorf("err: %v", err)
		}
	}
	<-done
}

func TestSession_WindowUpdateWriteDuringRead(t *testing.T) {
	client, server := testClientServerConfig(testConfNoKeepAlive())
	defer client.Close()
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Choose a huge flood size that we know will result in a window update.
	flood := int64(client.config.MaxStreamWindowSize) + 1

	sync := make(chan struct{})

	// The server will accept a new stream and then flood data to it.
	go func() {
		defer wg.Done()

		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			server.Close()
			return
		}
		defer stream.Close()

		<-sync
		sync <- struct{}{}

		_, err = stream.Write(make([]byte, flood))
		if err == nil {
			t.Errorf("expected write to fail due to no window update")
			return
		}
	}()

	// The client will open a stream, block outbound writes, and then
	// listen to the flood from the server, which should time out since
	// it won't be able to send the window update.
	go func() {
		defer wg.Done()

		stream, err := client.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			server.Close()
			return
		}
		defer stream.Close()

		sync <- struct{}{}
		conn := client.conn.(*pipeConn)
		conn.BlockWrites()
		<-sync

		_, err = io.ReadFull(stream, make([]byte, flood))
		if err == nil {
			t.Errorf("expected read to fail")
		}
	}()

	wg.Wait()
}

func TestSession_PartialReadWindowUpdate(t *testing.T) {
	client, server := testClientServerConfig(testConfNoKeepAlive())
	defer client.Close()
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Choose a huge flood size that we know will result in a window update.
	flood := int64(initialStreamWindow)
	var wr *Stream

	// The server will accept a new stream and then flood data to it.
	go func() {
		defer wg.Done()

		var err error
		wr, err = server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		sendWindow := atomic.LoadUint32(&wr.sendWindow)
		if sendWindow != initialStreamWindow {
			t.Errorf("sendWindow: exp=%d, got=%d", initialStreamWindow, sendWindow)
		}

		n, err := wr.Write(make([]byte, flood))
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		if int64(n) != flood {
			t.Errorf("short write: %d", n)
			return
		}
		sendWindow = atomic.LoadUint32(&wr.sendWindow)
		if sendWindow != 0 {
			t.Errorf("sendWindow: exp=%d, got=%d", 0, sendWindow)
			return
		}
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	wg.Wait()

	_, err = io.ReadFull(stream, make([]byte, flood/2))
	if err != nil {
		t.Fatal(err)
	}

	var (
		expWithoutWindowIncrease = uint32(flood / 2)
		expWithWindowIncrease    = uint32(flood)
		sendWindow               uint32
	)

	// This test is racy. Wait a short period, then longer and longer. At
	// most ~1s.
	for i := 1; i < 15; i++ {
		time.Sleep(time.Duration(i*i) * time.Millisecond)
		sendWindow = atomic.LoadUint32(&wr.sendWindow)
		if sendWindow == expWithoutWindowIncrease || sendWindow == expWithWindowIncrease {
			return
		}
	}
	t.Errorf("sendWindow: exp=%d or %d, got=%d", expWithoutWindowIncrease, expWithWindowIncrease, sendWindow)
}

// func TestSession_WindowAutoSizing(t *testing.T) {
// 	const initialWindow uint32 = 10
// 	conf := testConfNoKeepAlive()
// 	conf.InitialStreamWindowSize = initialWindow
// 	client, server := testClientServerConfig(conf)
// 	defer client.Close()
// 	defer server.Close()

// 	receiveAndConsume := func(str *Stream, size uint32) {
// 		if _, err := str.Read(make([]byte, size)); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	clientStr, err := client.OpenStream(context.Background())
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	serverStr, err := server.AcceptStream()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	const rtt = 20 * time.Millisecond
// 	t.Run("finding the window size", func(t *testing.T) {
// 		// Consume a maximum of 1234 bytes per RTT.
// 		// We expect the window to be scaled such that we send one update every 2 RTTs.
// 		go func() {
// 			for {
// 				serverStr.Write(make([]byte, 100))
// 			}
// 		}()

// 		var counter int
// 		ticker := time.NewTicker(rtt)
// 		for range ticker.C {
// 			receiveAndConsume(clientStr, 1234)
// 			counter++
// 			if counter > 25 {
// 				break
// 			}
// 		}
// 		fmt.Println(clientStr.recvWindow)
// 	})
// 	// t.Run("capping the window size", func(t *testing.T) {
// 	// 	const maxWindow = 78 * initialWindow
// 	// 	buf := newSegmentedBuffer(initialWindow, maxWindow, func() time.Duration { return rtt })
// 	// 	start := time.Now()
// 	// 	// Consume a maximum of 1234 bytes per RTT.
// 	// 	// We expect the window to be scaled such that we send one update every 2 RTTs.
// 	// 	now := start
// 	// 	delta := initialWindow
// 	// 	for i := 0; i < 100; i++ {
// 	// 		now = now.Add(rtt)
// 	// 		receiveAndConsume(&buf, delta)
// 	// 		grow, d := buf.GrowTo(false, now)
// 	// 		if grow {
// 	// 			delta = d
// 	// 		}
// 	// 	}
// 	// 	if buf.windowSize != maxWindow {
// 	// 		t.Fatalf("expected the window size to be at max (%d), got %d", maxWindow, buf.windowSize)
// 	// 	}
// 	// })
// }

func TestSession_sendMsg_Timeout(t *testing.T) {
	client, server := testClientServerConfig(testConfNoKeepAlive())
	defer client.Close()
	defer server.Close()

	conn := client.conn.(*pipeConn)
	conn.BlockWrites()

	hdr := encode(typePing, flagACK, 0, 0)
	for {
		err := client.sendMsg(hdr, nil, nil, true)
		if err == nil {
			continue
		} else if err == ErrConnectionWriteTimeout {
			break
		} else {
			t.Fatalf("err: %v", err)
		}
	}
}

func newPriorityQueueTestSession(priorityMessageSize uint32) *Session {
	return &Session{
		config: &Config{PriorityMessageSize: priorityMessageSize},
		sendCh: make(chan []byte, 64), priorityCh: make(chan []byte, 64),
		pingCh: make(chan uint32), pongCh: make(chan uint32, 1),
		shutdownCh: make(chan struct{}), sendDoneCh: make(chan struct{}),
	}
}

func TestSessionSendMsgPriorityClassification(t *testing.T) {
	const threshold = uint32(8)

	tests := []struct {
		name       string
		threshold  uint32
		hdr        header
		bodyLength int
		wantQueue  string
	}{
		{name: "small data stays ordered", threshold: threshold, hdr: encode(typeData, 0, 1, 0), bodyLength: int(threshold), wantQueue: "normal"},
		{name: "large data", threshold: threshold, hdr: encode(typeData, 0, 2, 0), bodyLength: int(threshold) + 1, wantQueue: "normal"},
		{name: "control frame", threshold: threshold, hdr: encode(typeWindowUpdate, 0, 3, 0), wantQueue: "priority"},
		{name: "disabled", threshold: 0, hdr: encode(typeData, 0, 4, 0), bodyLength: 1, wantQueue: "normal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPriorityQueueTestSession(tt.threshold)
			body := make([]byte, tt.bodyLength)
			if err := s.sendMsg(tt.hdr, body, nil, true); err != nil {
				t.Fatalf("sendMsg: %v", err)
			}

			var got []byte
			switch tt.wantQueue {
			case "priority":
				got = <-s.priorityCh
				if len(s.sendCh) != 0 {
					t.Fatal("priority message was also queued as normal")
				}
			case "normal":
				got = <-s.sendCh
				if len(s.priorityCh) != 0 {
					t.Fatal("normal message was queued as priority")
				}
			}
			poolPut(got)
		})
	}
}

type shortWriteRecorder struct {
	mu       sync.Mutex
	data     []byte
	writes   int
	expected int
	shutdown chan struct{}
	close    sync.Once
}

func (r *shortWriteRecorder) Read([]byte) (int, error) { return 0, io.EOF }

func (r *shortWriteRecorder) Write(data []byte) (int, error) {
	n := 3
	if len(data) < n {
		n = len(data)
	}
	r.mu.Lock()
	r.data = append(r.data, data[:n]...)
	r.writes++
	done := len(r.data) == r.expected
	r.mu.Unlock()
	if done {
		r.close.Do(func() { close(r.shutdown) })
	}
	return n, nil
}

func (r *shortWriteRecorder) Close() error {
	r.close.Do(func() { close(r.shutdown) })
	return nil
}

func (r *shortWriteRecorder) LocalAddr() net.Addr              { return testAddr("local") }
func (r *shortWriteRecorder) RemoteAddr() net.Addr             { return testAddr("remote") }
func (r *shortWriteRecorder) SetDeadline(time.Time) error      { return nil }
func (r *shortWriteRecorder) SetReadDeadline(time.Time) error  { return nil }
func (r *shortWriteRecorder) SetWriteDeadline(time.Time) error { return nil }

func TestSessionSendLoopCompletesShortWrites(t *testing.T) {
	frame := queuedDataFrame(7)
	shutdown := make(chan struct{})
	recorder := &shortWriteRecorder{expected: len(frame), shutdown: shutdown}
	s := newPriorityQueueTestSession(8)
	s.config.ConnectionWriteTimeout = time.Second
	s.conn = recorder
	s.shutdownCh = shutdown
	queued := poolGet(len(frame))
	copy(queued, frame)
	s.sendCh <- queued

	done := make(chan error, 1)
	go func() { done <- s.sendLoop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sendLoop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sendLoop did not finish the short writes")
	}
	recorder.mu.Lock()
	got := append([]byte(nil), recorder.data...)
	writes := recorder.writes
	recorder.mu.Unlock()
	if !bytes.Equal(got, frame) || writes <= 1 {
		t.Fatalf("short writes: got=%x want=%x writes=%d", got, frame, writes)
	}
}

type sendLoopRecorder struct {
	mu       sync.Mutex
	streamID []uint32
	shutdown chan struct{}
	close    sync.Once
}

func (r *sendLoopRecorder) Read([]byte) (int, error) { return 0, io.EOF }

func (r *sendLoopRecorder) Write(data []byte) (int, error) {
	if len(data) < headerSize {
		return 0, io.ErrShortBuffer
	}
	var hdr header
	copy(hdr[:], data[:headerSize])
	r.mu.Lock()
	r.streamID = append(r.streamID, hdr.StreamID())
	count := len(r.streamID)
	r.mu.Unlock()
	if count == 11 {
		r.close.Do(func() { close(r.shutdown) })
	}
	return len(data), nil
}

func (r *sendLoopRecorder) Close() error {
	r.close.Do(func() { close(r.shutdown) })
	return nil
}

func (r *sendLoopRecorder) LocalAddr() net.Addr              { return testAddr("local") }
func (r *sendLoopRecorder) RemoteAddr() net.Addr             { return testAddr("remote") }
func (r *sendLoopRecorder) SetDeadline(time.Time) error      { return nil }
func (r *sendLoopRecorder) SetReadDeadline(time.Time) error  { return nil }
func (r *sendLoopRecorder) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func queuedDataFrame(streamID uint32) []byte {
	hdr := encode(typeData, 0, streamID, 1)
	return append(hdr[:], byte(streamID))
}

func TestSessionPrioritySendLoopFairness(t *testing.T) {
	shutdown := make(chan struct{})
	recorder := &sendLoopRecorder{shutdown: shutdown}
	s := newPriorityQueueTestSession(8)
	s.config.ConnectionWriteTimeout = time.Second
	s.conn = recorder
	s.shutdownCh = shutdown

	for streamID := uint32(1); streamID <= 10; streamID++ {
		s.priorityCh <- queuedDataFrame(streamID)
	}
	s.sendCh <- queuedDataFrame(100)

	done := make(chan error, 1)
	go func() { done <- s.sendLoop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sendLoop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sendLoop did not drain the deterministic queue")
	}

	recorder.mu.Lock()
	got := append([]uint32(nil), recorder.streamID...)
	recorder.mu.Unlock()
	want := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 100, 9, 10}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("send order: got %v, want %v", got, want)
	}
}

func TestWindowOverflow(t *testing.T) {
	// Ensures:
	//
	// 1. We don't accept a message that's too big.
	// 2. We unlock after resetting the stream.
	for i := uint32(1); i < 100; i += 2 {
		func() {
			client, server := testClientServerConfig(testConfNoKeepAlive())
			defer client.Close()
			defer server.Close()

			hdr1 := encode(typeData, flagSYN, i, 0)
			_ = client.sendMsg(hdr1, nil, nil, true)
			s, err := server.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			msg := make([]byte, client.config.MaxStreamWindowSize*2)
			hdr2 := encode(typeData, 0, i, uint32(len(msg)))
			_ = client.sendMsg(hdr2, msg, nil, true)
			_, err = io.ReadAll(s)
			if err == nil {
				t.Fatal("expected to read no data")
			}
		}()
	}
}

func TestSession_ConnectionWriteTimeout(t *testing.T) {
	client, server := testClientServerConfig(testConfNoKeepAlive())
	defer client.Close()
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	sync := make(chan struct{})

	go func() {
		defer wg.Done()

		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		<-sync
		sync <- struct{}{}

		defer stream.Close()
	}()

	// The client will open the stream and then block outbound writes, we'll
	// tee up a write and make sure it eventually times out.
	go func() {
		defer wg.Done()

		stream, err := client.OpenStream(context.Background())
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		defer stream.Close()

		sync <- struct{}{}
		conn := client.conn.(*pipeConn)
		conn.BlockWrites()
		<-sync

		// Fill up the write queue and wait for the write to timeout.
		for {
			_, err := stream.Write([]byte("hello"))
			if err == nil {
				continue
			} else if err == ErrConnectionWriteTimeout {
				break
			} else {
				t.Errorf("err: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

func TestStreamResetWrite(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		stream, err := server.AcceptStream()
		if err != nil {
			t.Errorf("err: %v", err)
		}

		time.Sleep(time.Millisecond * 50)

		_, err = stream.Write([]byte("foo"))
		if err == nil {
			t.Errorf("should have failed to write")
		}
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = stream.Reset()
	if err != nil {
		t.Fatal(err)
	}
	<-wait
}

// Because reads should succeed after closing the stream.
func TestStreamHalfClose2(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wait := make(chan struct{})

	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			t.Error(err)
		}
		<-wait
		_, err = stream.Write([]byte("asdf"))
		if err != nil {
			t.Error(err)
		}
		stream.Close()
		wait <- struct{}{}
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Error(err)
	}
	defer stream.Close()

	stream.CloseWrite()
	wait <- struct{}{}

	buf, err := io.ReadAll(stream)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(buf, []byte("asdf")) {
		t.Fatalf("didn't get expected data")
	}
	<-wait
}

func TestStreamResetRead(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wc := new(sync.WaitGroup)
	wc.Add(2)
	go func() {
		defer wc.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Error(err)
		}

		_, err = io.ReadAll(stream)
		if err == nil {
			t.Errorf("expected reset")
		}
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Error(err)
	}

	go func() {
		defer wc.Done()

		_, err := io.ReadAll(stream)
		if err == nil {
			t.Errorf("expected reset")
		}
	}()

	time.Sleep(1 * time.Second)
	err = stream.Reset()
	if err != nil {
		t.Fatal(err)
	}
	wc.Wait()
}

func TestStreamResetWithError(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	wc := new(sync.WaitGroup)
	wc.Add(1)
	go func() {
		defer wc.Done()
		stream, err := server.AcceptStream()
		if err != nil {
			t.Error(err)
		}

		se := &StreamError{}
		_, err = io.ReadAll(stream)
		if !errors.As(err, &se) {
			t.Errorf("expected StreamError, got type:%T, err: %s", err, err)
			return
		}
		expected := &StreamError{Remote: true, ErrorCode: 42}
		assert.Equal(t, se, expected)
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Second)
	err = stream.ResetWithError(42)
	if err != nil {
		t.Fatal(err)
	}
	se := &StreamError{}
	_, err = io.ReadAll(stream)
	if !errors.As(err, &se) {
		t.Errorf("expected StreamError, got type:%T, err: %s", err, err)
		return
	}
	expected := &StreamError{Remote: false, ErrorCode: 42}
	assert.Equal(t, se, expected)
	wc.Wait()
}

func TestLotsOfWritesWithStreamDeadline(t *testing.T) {
	config := testConf()
	config.EnableKeepAlive = false

	client, server := testClientServerConfig(config)
	defer client.Close()
	defer server.Close()

	waitCh := make(chan struct{})
	doneCh := make(chan struct{})

	// Server side accepts two streams. The first one is the clogger.
	go func() {
		defer close(doneCh)
		_, err := server.AcceptStream()
		if err != nil {
			t.Error(err)
			return
		}

		stream2, err := server.AcceptStream()
		if err != nil {
			t.Error(err)
			return
		}

		waitCh <- struct{}{}

		// Wait until all writes have timed out on the client.
		<-waitCh

		// stream2 should've received no messages, as they all expired in the buffer.
		err = stream2.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if err != nil {
			t.Error(err)
			return
		}
		if b, err := io.ReadAll(stream2); len(b) != 0 || err != ErrTimeout {
			t.Errorf("writes from the client should've expired; got: %v, bytes: %v", err, b)
			return
		}
	}()

	// stream1 is the clogger.
	stream1, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// all writes on stream2 will time out.
	stream2, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream2.Reset() // nolint

	// wait for the server to accept the streams.
	<-waitCh

	clientConn := client.conn.(*pipeConn)
	clientConn.BlockWrites()

	// Send a clogging write on stream1.
	go func() {
		err := stream1.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = stream1.Write([]byte{100})
	}()

	// Keep writing till we fill the buffer and timeout.
	var wg sync.WaitGroup
	err = stream2.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := stream2.Write([]byte("foobar"))
		if err == nil {
			continue
		} else if err == ErrTimeout {
			break
		} else {
			t.Errorf("expected stream timeout error, got: %v", err)
			break
		}
	}

	// All writes completed and timed out; notify the server.
	wg.Wait()
	select {
	case waitCh <- struct{}{}:
	default:
	}
	<-doneCh
}

func TestReadDeadlineInterrupt(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	stream, err := client.Open(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream2.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4)
		if _, err := stream.Read(buf); err != ErrTimeout {
			t.Errorf("err: %v", err)
			return
		}
	}()

	select {
	case <-done:
		t.Fatal("read shouldn't have finished")
	case <-time.After(5 * time.Millisecond):
	}

	if err := stream.SetReadDeadline(time.Now().Add(5 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("read should have finished")
	}

	for i := 0; i < 5; i++ {
		buf := make([]byte, 4)
		if _, err := stream.Read(buf); err != ErrTimeout {
			t.Fatalf("err: %v", err)
		}
	}
}

// Make sure that a transfer doesn't stall, no matter what values the peers use for their InitialStreamWindow.
func TestInitialStreamWindow(t *testing.T) {
	for i := 0; i < 10; i++ {
		const (
			maxWindow    = 5 * initialStreamWindow
			transferSize = 10 * maxWindow
		)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randomUint32 := func(min, max uint32) uint32 { return uint32(r.Int63n(int64(max-min))) + min }

		cconf := DefaultConfig()
		cconf.InitialStreamWindowSize = randomUint32(initialStreamWindow, maxWindow)
		sconf := DefaultConfig()
		sconf.InitialStreamWindowSize = randomUint32(initialStreamWindow, maxWindow)

		conn1, conn2 := testConn()
		client, _ := Client(conn1, cconf, nil)
		server, _ := Server(conn2, sconf, nil)

		errChan := make(chan error, 1)
		go func() {
			defer close(errChan)
			str, err := client.OpenStream(context.Background())
			if err != nil {
				errChan <- err
				return
			}
			defer str.Close()
			if _, err := str.Write(make([]byte, transferSize)); err != nil {
				errChan <- err
				return
			}
		}()

		str, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(str)
		if err != nil {
			t.Fatal(err)
		}
		if uint32(len(data)) != transferSize {
			t.Fatalf("expected %d bytes to be transferred, got %d", transferSize, len(data))
		}
	}
}

func TestMaxIncomingStreams(t *testing.T) {
	const maxIncomingStreams = 5
	conn1, conn2 := testConn()
	client, err := Client(conn1, DefaultConfig(), nil)
	require.NoError(t, err)
	defer client.Close()

	conf := DefaultConfig()
	conf.MaxIncomingStreams = maxIncomingStreams
	server, err := Server(conn2, conf, nil)
	require.NoError(t, err)
	defer server.Close()

	strChan := make(chan *Stream, maxIncomingStreams)
	go func() {
		defer close(strChan)
		for {
			str, err := server.AcceptStream()
			if err != nil {
				return
			}
			_, err = str.Write([]byte("foobar"))
			require.NoError(t, err)
			strChan <- str
		}
	}()

	for i := 0; i < maxIncomingStreams; i++ {
		str, err := client.OpenStream(context.Background())
		require.NoError(t, err)
		_, err = str.Read(make([]byte, 6))
		require.NoError(t, err)
		require.NoError(t, str.CloseWrite())
	}
	// The server now has maxIncomingStreams incoming streams.
	// It will now reset the next stream that is opened.
	str, err := client.OpenStream(context.Background())
	require.NoError(t, err)
	str.SetDeadline(time.Now().Add(time.Second))
	_, err = str.Read([]byte{0})
	require.ErrorIs(t, err, ErrStreamReset)

	// Now close one of the streams.
	// This should then allow the client to open a new stream.
	require.NoError(t, (<-strChan).Close())
	str, err = client.OpenStream(context.Background())
	require.NoError(t, err)
	str.SetDeadline(time.Now().Add(time.Second))
	_, err = str.Read([]byte{0})
	require.NoError(t, err)
}

func TestErrorCodeErrorIsErrStreamReset(t *testing.T) {
	se := &StreamError{}
	require.True(t, errors.Is(se, ErrStreamReset))
	ge := &GoAwayError{}
	require.True(t, errors.Is(ge, ErrStreamReset))
}

func testTCPConn(t *testing.T) (net.Conn, net.Conn) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Channel to receive the server-side connection
	serverConnCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)

	// Accept connection in goroutine
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		serverConnCh <- conn
	}()

	// Connect to the listener
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	// Wait for server connection or error
	select {
	case serverConn := <-serverConnCh:
		return clientConn, serverConn
	case err := <-errCh:
		clientConn.Close()
		t.Fatal(err)
		return nil, nil
	case <-time.After(time.Second):
		clientConn.Close()
		t.Fatal("timeout waiting for connection")
		return nil, nil
	}
}

// TestSessionCloseDeadlock is a regression test for a deadlock on closing connections.
// See: https://github.com/libp2p/go-yamux/issues/129 for details
func TestSessionCloseDeadlock(t *testing.T) {
	const n = 10
	const numWrites = 1000
	var closeWG sync.WaitGroup
	closeWG.Add(numWrites * n)
	for i := 0; i < n; i++ {
		go func() {
			conn1, conn2 := testTCPConn(t)
			conf := DefaultConfig()
			conf.LogOutput = io.Discard
			client, err := Client(conn1, conf, nil)
			require.NoError(t, err)
			defer client.Close()

			conf = DefaultConfig()
			conf.LogOutput = io.Discard
			server, err := Server(conn2, conf, nil)
			require.NoError(t, err)
			defer server.Close()

			// Create a stream from client
			stream, err := client.OpenStream(context.Background())
			require.NoError(t, err)
			defer stream.Close()

			// Accept the stream on server side
			serverStream, err := server.AcceptStream()
			require.NoError(t, err)
			defer serverStream.Close()

			// Send an incomplete dataframe to the server to ensure that the recvloop is reading the rest
			// of the message.
			buf := make([]byte, 1<<10)
			hdr := encode(typeData, serverStream.sendFlags(), serverStream.id, uint32(len(buf))+1000)
			err = server.sendMsg(hdr, buf, nil, true)
			require.NoError(t, err)

			time.Sleep(1 * time.Second) // Let the read loop block on reading rest of the data frame

			// Make many writes so that these writes takes up all the buffer space in the channel
			// and the recvLoop deadlocks because it's unable to send the goAwayProtoErr
			var writeWG sync.WaitGroup
			writeWG.Add(numWrites)
			for i := 0; i < numWrites; i++ {
				go func() {
					defer closeWG.Done()
					buf := make([]byte, 1) // make small writes so that we queue up the channel
					writeWG.Done()         // This is used to trigger concurrent streamWrite with CloseWrite.
					stream.Write(buf)
				}()
			}
			// Wait for the client to exit
			writeWG.Wait()
			// Close the Write half of the client conn so that the send loop exits first.
			err = conn1.(*net.TCPConn).CloseWrite()
			require.NoError(t, err)
		}()
	}
	closeWG.Wait()
}
