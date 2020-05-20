package rudp

import (
	"errors"
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/frame"
	"github.com/cxjava/go-engine/loggo"
	"github.com/golang/protobuf/proto"
	"net"
	"time"
)

func Listen(addr string, cc *ConnConfig) (*Conn, error) {
	if cc == nil {
		cc = &ConnConfig{}
	}
	cc.Check()

	ipaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		loggo.Debug("Error Resolve addr for udp packets: %s %s", addr, err.Error())
		return nil, err
	}

	listener, err := net.ListenUDP("udp", ipaddr)
	if err != nil {
		loggo.Debug("Error listening for udp packets: %s %s", addr, err.Error())
		return nil, err
	}

	conn := &Conn{}
	conn.isListener = true
	conn.config = *cc
	conn.conn = listener
	conn.localAddr = listener.LocalAddr().String()
	conn.inited = true
	conn.waitAccept = make(chan *Conn, cc.Backlog)
	conn.id = common.Guid()

	go conn.updateListener(cc)

	return conn, nil
}

func (conn *Conn) updateListener(cc *ConnConfig) {

	defer common.CrashLog()

	conn.workResultLock.Add(1)
	defer conn.workResultLock.Done()

	loggo.Info("start rudp listener %s", conn.localAddr)

	bytes := make([]byte, 2000)

	for !conn.exit && !conn.closed {
		conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
		n, srcaddr, err := conn.conn.ReadFromUDP(bytes)
		if err != nil {
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Timeout() {
				loggo.Debug("Error read udp %s", err)
				time.Sleep(time.Millisecond * 100)
				continue
			}
		}
		if n <= 0 {
			continue
		}

		clientConn := conn.getClientConnByAddr(srcaddr.String())
		if clientConn == nil {
			if !conn.noaccept {
				clientConn := &Conn{}
				clientConn.config = conn.config
				clientConn.localAddr = conn.localAddr
				clientConn.remoteAddr = srcaddr.String()
				clientConn.conn = conn.conn
				clientConn.id = common.Guid()

				fm := frame.NewFrameMgr(RUDP_MAX_SIZE, RUDP_MAX_ID, conn.config.BufferSize, conn.config.MaxWin, conn.config.ResendTimems, conn.config.Compress, conn.config.Stat)
				clientConn.fm = fm
				clientConn.fm.SetDebugid(clientConn.Id())

				conn.addClientConn(srcaddr.String(), clientConn)

				go conn.accept(clientConn, srcaddr, cc)
			} else {
				loggo.Info("server read no connected data from %s", srcaddr.String())
			}
		} else {
			f := &frame.Frame{}
			err := proto.Unmarshal(bytes[0:n], f)
			if err == nil {
				clientConn.fm.OnRecvFrame(f)
			} else {
				loggo.Error("Unmarshal fail from %s %s", srcaddr.String(), err)
			}
		}
	}
}

func (conn *Conn) accept(c *Conn, addr *net.UDPAddr, cc *ConnConfig) {
	defer common.CrashLog()

	conn.workResultLock.Add(1)
	defer conn.workResultLock.Done()

	loggo.Info("server begin accept remote rudp %s->%s", c.remoteAddr, c.localAddr)

	startConnectTime := time.Now()
	done := false
	for !conn.exit && !conn.closed {
		oc := conn.getClientConnByAddr(addr.String())
		if oc == nil || oc.Id() != c.Id() {
			return
		}

		if c.fm.IsConnected() {
			done = true
			break
		}

		c.fm.Update()

		// send udp
		sendlist := c.fm.GetSendList()
		for e := sendlist.Front(); e != nil; e = e.Next() {
			f := e.Value.(*frame.Frame)
			mb, _ := c.fm.MarshalFrame(f)
			c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
			c.conn.WriteToUDP(mb, addr)
		}

		now := time.Now()
		diffclose := now.Sub(startConnectTime)
		if diffclose > time.Millisecond*time.Duration(cc.ConnectTimeoutMs) {
			loggo.Debug("can not connect by remote rudp %s->%s", c.remoteAddr, c.localAddr)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	if !done {
		conn.deleteClientConn(addr.String())
		return
	}

	c.inited = true

	loggo.Info("server accept ok remote rudp %s->%s", c.remoteAddr, c.localAddr)

	go c.updateServer(conn, addr)
	conn.waitAccept <- c
}

func (conn *Conn) Accept(timeoutms int) *Conn {
	if !conn.isListener {
		return nil
	}
	if timeoutms > 0 {
		select {
		case c := <-conn.waitAccept:
			return c
		case <-time.After(time.Duration(timeoutms) * time.Millisecond):
			return nil
		}
	} else {
		select {
		case c := <-conn.waitAccept:
			return c
		}
	}
}

func (conn *Conn) updateServer(fconn *Conn, addr *net.UDPAddr) {
	defer common.CrashLog()

	fconn.workResultLock.Add(1)
	defer fconn.workResultLock.Done()

	conn.workResultLock.Add(1)
	defer conn.workResultLock.Done()

	loggo.Info("start rudp conn %s->%s", conn.remoteAddr, conn.localAddr)

	for !conn.exit && !fconn.exit && !conn.closed && !fconn.closed {
		oc := fconn.getClientConnByAddr(addr.String())
		if oc == nil || oc.Id() != conn.Id() {
			return
		}

		sleep := true

		conn.fm.Update()

		// send udp
		sendlist := conn.fm.GetSendList()
		if sendlist.Len() > 0 {
			sleep = false
			for e := sendlist.Front(); e != nil; e = e.Next() {
				f := e.Value.(*frame.Frame)
				mb, _ := conn.fm.MarshalFrame(f)
				conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
				conn.conn.WriteToUDP(mb, addr)
			}
		}

		// timeout
		if conn.fm.IsHBTimeout(conn.config.HBTimeoutms) {
			loggo.Debug("close inactive conn %s->%s", conn.remoteAddr, conn.localAddr)
			conn.fm.Close()
			break
		}

		if conn.fm.IsRemoteClosed() {
			loggo.Debug("closed by remote conn %s->%s", conn.remoteAddr, conn.localAddr)
			conn.fm.Close()
			break
		}

		if sleep {
			time.Sleep(time.Millisecond * 10)
		}
	}

	conn.fm.Close()
	conn.closed = true

	startCloseTime := time.Now()
	for !conn.exit && !fconn.exit {
		now := time.Now()

		conn.fm.Update()

		// send udp
		sendlist := conn.fm.GetSendList()
		for e := sendlist.Front(); e != nil; e = e.Next() {
			f := e.Value.(*frame.Frame)
			mb, _ := conn.fm.MarshalFrame(f)
			conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 10))
			conn.conn.WriteToUDP(mb, addr)
		}

		diffclose := now.Sub(startCloseTime)
		if diffclose > time.Millisecond*time.Duration(conn.config.CloseTimeoutMs) {
			loggo.Info("close conn had timeout %s->%s", conn.remoteAddr, conn.localAddr)
			break
		}

		remoteclosed := conn.fm.IsRemoteClosed()
		if remoteclosed {
			loggo.Info("remote conn had closed %s->%s", conn.remoteAddr, conn.localAddr)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	conn.exit = true

	loggo.Info("close rudp conn %s->%s", conn.remoteAddr, conn.localAddr)

	time.Sleep(time.Second)

	fconn.deleteClientConn(addr.String())

	loggo.Info("delete rudp conn %s->%s", conn.remoteAddr, conn.localAddr)
}

func (conn *Conn) Dail(targetAddr string) (*Conn, error) {
	return conn.DailWithTimeout(targetAddr, conn.config.ConnectTimeoutMs)
}

func (conn *Conn) DailWithTimeout(targetAddr string, timeoutms int) (*Conn, error) {

	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		loggo.Debug("Error Resolve addr for udp packets: %s %s", addr, err.Error())
		return nil, err
	}

	targetAddr = addr.String()
	if targetAddr == conn.localAddr {
		return nil, errors.New("can not connect self rudp " + targetAddr)
	}

	clientConn := conn.getClientConnByAddr(targetAddr)
	if clientConn != nil && clientConn.IsConnected() {
		return clientConn, nil
	}
	startConnectTime := time.Now()

	clientConn = &Conn{}
	clientConn.config = conn.config
	clientConn.localAddr = conn.localAddr
	clientConn.remoteAddr = targetAddr
	clientConn.conn = conn.conn
	clientConn.id = common.Guid()

	fm := frame.NewFrameMgr(RUDP_MAX_SIZE, RUDP_MAX_ID, conn.config.BufferSize, conn.config.MaxWin, conn.config.ResendTimems, conn.config.Compress, conn.config.Stat)
	clientConn.fm = fm
	clientConn.fm.SetDebugid(clientConn.Id())

	conn.addClientConn(targetAddr, clientConn)

	fm.Connect()
	done := false
	for !conn.exit && !conn.closed {
		if fm.IsConnected() {
			done = true
			break
		}

		fm.Update()

		// send udp
		sendlist := clientConn.fm.GetSendList()
		if sendlist.Len() > 0 {
			for e := sendlist.Front(); e != nil; e = e.Next() {
				f := e.Value.(*frame.Frame)
				mb, _ := clientConn.fm.MarshalFrame(f)
				clientConn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
				clientConn.conn.WriteToUDP(mb, addr)
			}
		}

		// timeout
		now := time.Now()
		diffclose := now.Sub(startConnectTime)
		if diffclose > time.Millisecond*time.Duration(timeoutms) {
			loggo.Debug("can not connect remote rudp %s", targetAddr)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	if !done {
		conn.deleteClientConn(targetAddr)
		return nil, errors.New("can not connect remote rudp " + targetAddr)
	}

	clientConn.inited = true

	loggo.Info("server connect ok remote rudp %s->%s", clientConn.localAddr, clientConn.remoteAddr)

	go clientConn.updateServer(conn, addr)

	return clientConn, nil
}
