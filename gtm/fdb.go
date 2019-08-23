package gtm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

var (
	ErrStopped = errors.New("fdbStreamClient: stream listener stopped")
)

// processor - process one message
type processor func(msg []byte) error

// FDBStreamClient - FDBDoc change listener
type FDBStreamClient struct {
	address []string
	timeout time.Duration
	conn    net.Conn
	stop    chan struct{}
	enabled bool
	logger  *log.Logger
	mtx     sync.Mutex
}

// NewFDBStreamClient - create new fdb doc change stream listener
func NewFDBStreamClient(urls []string, timeout string, logger *log.Logger) FDBStreamClient {
	deadlineTimeout, err := time.ParseDuration(timeout)
	if err != nil {
		logger.Println("fdbStreamClient error: unable to parse timeout duration")
		deadlineTimeout = time.Second * 120
	}

	client := FDBStreamClient{address: urls, timeout: deadlineTimeout, logger: logger}
	client.stop = make(chan struct{})
	return client
}

// Stop - stop listening
func (f *FDBStreamClient) Stop() bool {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if !f.enabled {
		return false
	}

	f.stop <- struct{}{}
	f.enabled = false

	return true
}

// Listen - listen and read connection
func (f *FDBStreamClient) Listen(processor processor) error {
	f.mtx.Lock()
	f.enabled = true
	f.mtx.Unlock()

	if err := f.connect(); err != nil {
		return err
	}

	defer func() { _ = f.close() }()
	if err := f.receive(processor); err != nil {
		return err
	}

	return nil
}

// connect - establish connection with fdb change stream
func (f *FDBStreamClient) connect() (err error) {
	for _, address := range f.address {
		f.conn, err = net.Dial("tcp", address)
		if err == nil {
			f.logger.Println("fdbStreamClient: connected to", address)
			return
		}

		f.logger.Println("fdbStreamClient: unable to connect to", address)
	}

	return
}

// refresh - refresh connection timeout
func (f *FDBStreamClient) refresh() error {
	return f.conn.SetDeadline(time.Now().Add(f.timeout))
}

// close - close connection
func (f *FDBStreamClient) close() error {
	if f.conn == nil {
		return nil
	}
	return f.conn.Close()
}

// receive - receive data from connection
func (f *FDBStreamClient) receive(processor processor) error {
	buf := bufio.NewReader(f.conn)

	for {
		if f.isStopSignal() {
			return ErrStopped
		}

		if err := f.refresh(); err != nil {
			return err
		}

		lnBytes, err := buf.ReadBytes('\n')
		if err != nil {
			return err
		}

		msgSize := binary.LittleEndian.Uint64(lnBytes)
		msgBytes := make([]byte, msgSize)
		if _, err = io.ReadFull(buf, msgBytes); err != nil {
			return err
		}

		if err = processor(msgBytes); err != nil {
			return err
		}
	}
}

// isStopSignal - return true if stopped
func (f *FDBStreamClient) isStopSignal() bool {
	select {
	case <-f.stop:
		return true
	default:
		return false
	}
}
