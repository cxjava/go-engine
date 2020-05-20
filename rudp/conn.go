package rudp

import (
	"errors"
	"github.com/cxjava/go-engine/frame"
	"net"
	"sync"
)

const (
	RUDP_MAX_SIZE int = 500
	RUDP_MAX_ID   int = 100000
)

type ConnConfig struct {
	BufferSize       int
	MaxWin           int
	ResendTimems     int
	Compress         int
	Stat             int
	HBTimeoutms      int
	Backlog          int
	ConnectTimeoutMs int
	CloseTimeoutMs   int
}

func (cc *ConnConfig) Check() {
	if cc.BufferSize == 0 {
		cc.BufferSize = 1024 * 1024
	}
	if cc.MaxWin == 0 {
		cc.MaxWin = 1000
	}
	if cc.ResendTimems == 0 {
		cc.ResendTimems = 400
	}
	if cc.HBTimeoutms == 0 {
		cc.HBTimeoutms = 3000
	}
	if cc.Backlog == 0 {
		cc.Backlog = 100
	}
	if cc.ConnectTimeoutMs == 0 {
		cc.ConnectTimeoutMs = 1000
	}
	if cc.CloseTimeoutMs == 0 {
		cc.CloseTimeoutMs = 1000
	}
}

type Conn struct {
	isClient   bool
	isListener bool
	config     ConnConfig
	inited     bool
	exit       bool
	closed     bool
	localAddr  string
	remoteAddr string

	conn *net.UDPConn
	fm   *frame.FrameMgr

	workResultLock sync.WaitGroup

	localAddrToConnMap sync.Map
	waitAccept         chan *Conn

	username string
	userdata interface{}
	id       string

	noaccept bool
}

func (conn *Conn) SetUsername(username string) {
	conn.username = username
}

func (conn *Conn) Username() string {
	return conn.username
}

func (conn *Conn) SetNoaccept(noaccept bool) {
	conn.noaccept = noaccept
}

func (conn *Conn) Id() string {
	return conn.id
}

func (conn *Conn) RemoteAddr() string {
	return conn.remoteAddr
}

func (conn *Conn) LocalAddr() string {
	return conn.localAddr
}

func (conn *Conn) Close(force bool) {
	if conn.exit || conn.closed {
		return
	}
	if force {
		conn.exit = true
	}
	conn.closed = true
	conn.workResultLock.Wait()
	if conn.isListener {
		conn.conn.Close()
	}
}

func (conn *Conn) IsConnected() bool {
	return !conn.exit && conn.inited
}

func (conn *Conn) SetUserData(userdata interface{}) {
	conn.userdata = userdata
}

func (conn *Conn) UserData() interface{} {
	return conn.userdata
}

func (conn *Conn) Write(bytes []byte) (int, error) {

	if conn.exit {
		return 0, errors.New("write on closed conn " + conn.localAddr + "->" + conn.remoteAddr)
	}

	size := len(bytes)
	if size > conn.fm.GetSendBufferLeft() {
		size = conn.fm.GetSendBufferLeft()
	}

	if size <= 0 {
		return 0, nil
	}

	conn.fm.WriteSendBuffer(bytes[0:size])
	return size, nil
}

func (conn *Conn) Read(bytes []byte) (int, error) {

	if conn.exit {
		return 0, errors.New("read on closed conn " + conn.localAddr + "->" + conn.remoteAddr)
	}

	if conn.fm.GetRecvBufferSize() <= 0 {
		return 0, nil
	}

	size := copy(bytes, conn.fm.GetRecvReadLineBuffer())
	conn.fm.SkipRecvBuffer(size)
	return size, nil
}

func (conn *Conn) addClientConn(addr string, c *Conn) {
	conn.localAddrToConnMap.Store(addr, c)
}

func (conn *Conn) getClientConnByAddr(addr string) *Conn {
	ret, ok := conn.localAddrToConnMap.Load(addr)
	if !ok {
		return nil
	}
	return ret.(*Conn)
}

func (conn *Conn) deleteClientConn(addr string) {
	conn.localAddrToConnMap.Delete(addr)
}
