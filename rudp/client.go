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

func Dail(targetAddr string, cc *ConnConfig) (*Conn, error) {
	if cc == nil {
		cc = &ConnConfig{}
	}
	cc.Check()
	return DailWithTimeout(targetAddr, cc, cc.ConnectTimeoutMs)
}

func DailWithTimeout(targetAddr string, cc *ConnConfig, timeoutms int) (*Conn, error) {
	if cc == nil {
		cc = &ConnConfig{}
	}
	cc.Check()

	conn := &Conn{}

	startConnectTime := time.Now()
	c, err := net.DialTimeout("udp", targetAddr, time.Millisecond*time.Duration(timeoutms/2))
	if err != nil {
		loggo.Debug("Error listening for udp packets: %s %s", targetAddr, err.Error())
		return nil, err
	}
	targetConn := c.(*net.UDPConn)
	conn.config = *cc
	conn.conn = targetConn
	conn.remoteAddr = targetConn.RemoteAddr().String()
	conn.isClient = true
	conn.id = common.Guid()

	fm := frame.NewFrameMgr(RUDP_MAX_SIZE, RUDP_MAX_ID, cc.BufferSize, cc.MaxWin, cc.ResendTimems, cc.Compress, cc.Stat)
	conn.fm = fm
	conn.fm.SetDebugid(conn.Id())

	fm.Connect()
	bytes := make([]byte, 2000)
	for {
		if fm.IsConnected() {
			break
		}

		fm.Update()

		// send udp
		sendlist := fm.GetSendList()
		for e := sendlist.Front(); e != nil; e = e.Next() {
			f := e.Value.(*frame.Frame)
			mb, _ := fm.MarshalFrame(f)
			conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
			conn.conn.Write(mb)
		}

		// recv udp
		conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
		n, _ := conn.conn.Read(bytes)
		if n > 0 {
			f := &frame.Frame{}
			err := proto.Unmarshal(bytes[0:n], f)
			if err == nil {
				fm.OnRecvFrame(f)
			} else {
				loggo.Error("Unmarshal fail from %s %s", targetAddr, err)
			}
		}

		// timeout
		now := time.Now()
		diffclose := now.Sub(startConnectTime)
		if diffclose > time.Millisecond*time.Duration(timeoutms) {
			loggo.Debug("can not connect remote rudp %s", targetAddr)
			return nil, errors.New("can not connect remote rudp " + targetAddr)
		}

		time.Sleep(time.Millisecond * 10)
	}

	conn.localAddr = conn.conn.LocalAddr().String()
	conn.inited = true

	go conn.updateClient()

	return conn, nil
}

func (conn *Conn) updateClient() {

	defer common.CrashLog()

	conn.workResultLock.Add(1)
	defer conn.workResultLock.Done()

	loggo.Info("start rudp conn %s->%s", conn.localAddr, conn.remoteAddr)

	bytes := make([]byte, 2000)

	for !conn.exit && !conn.closed {
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
				conn.conn.Write(mb)
			}
		}

		// recv udp
		conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
		n, _ := conn.conn.Read(bytes)
		if n > 0 {
			f := &frame.Frame{}
			err := proto.Unmarshal(bytes[0:n], f)
			if err == nil {
				conn.fm.OnRecvFrame(f)
			} else {
				loggo.Error("Unmarshal fail from %s %s", conn.remoteAddr, err)
			}
		}

		// timeout
		if conn.fm.IsHBTimeout(conn.config.HBTimeoutms) {
			loggo.Debug("close inactive conn %s->%s", conn.localAddr, conn.remoteAddr)
			conn.fm.Close()
			break
		}

		if conn.fm.IsRemoteClosed() {
			loggo.Debug("closed by remote conn %s->%s", conn.localAddr, conn.remoteAddr)
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
	for !conn.exit {
		now := time.Now()

		conn.fm.Update()

		// send udp
		sendlist := conn.fm.GetSendList()
		for e := sendlist.Front(); e != nil; e = e.Next() {
			f := e.Value.(*frame.Frame)
			mb, _ := conn.fm.MarshalFrame(f)
			conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 10))
			conn.conn.Write(mb)
		}

		// recv udp
		conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 10))
		n, _ := conn.conn.Read(bytes)
		if n > 0 {
			f := &frame.Frame{}
			err := proto.Unmarshal(bytes[0:n], f)
			if err == nil {
				conn.fm.OnRecvFrame(f)
			} else {
				loggo.Error("Unmarshal fail from %s %s", conn.remoteAddr, err)
			}
		}

		diffclose := now.Sub(startCloseTime)
		if diffclose > time.Millisecond*time.Duration(conn.config.CloseTimeoutMs) {
			loggo.Info("close conn had timeout %s->%s", conn.localAddr, conn.remoteAddr)
			break
		}

		remoteclosed := conn.fm.IsRemoteClosed()
		if remoteclosed {
			loggo.Info("remote conn had closed %s->%s", conn.localAddr, conn.remoteAddr)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	conn.exit = true
	conn.conn.Close()

	loggo.Info("close rudp conn %s->%s", conn.localAddr, conn.remoteAddr)
}
