package evilnet

import (
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/loggo"
	"github.com/cxjava/go-engine/msgmgr"
	"github.com/cxjava/go-engine/rpc"
	"github.com/cxjava/go-engine/rudp"
	"github.com/golang/protobuf/proto"
	"strconv"
	"sync"
	"time"
)

type EvilNetConfig struct {
	Name string

	ListenSonPort int
	ListenOutPort int
	Fatheraddr    string

	RegFatherInterSec int

	Key        string
	FatherKey  string
	ConnectKey string

	Rudpconfig rudp.ConnConfig
}

func (evc *EvilNetConfig) Check() {
	if len(evc.Name) <= 0 {
		evc.Name = common.RandStr(6)
	}
	if len(evc.ConnectKey) <= 0 {
		evc.ConnectKey = common.RandStr(16)
	}
	if evc.RegFatherInterSec <= 0 {
		evc.RegFatherInterSec = 10
	}
	evc.Rudpconfig.Check()
}

type EvilNet struct {
	exit           bool
	config         *EvilNetConfig
	workResultLock sync.WaitGroup

	uuid    string
	localip string

	fa     *rudp.Conn
	father *rudp.Conn

	fathername string
	globalname string
	globaladdr string

	son        *rudp.Conn
	sonConnMap sync.Map
	sonid      int

	plugin map[string]PluginCreator

	curConnRandomKey string
}

func (ev *EvilNet) Globalname() string {
	return ev.globalname
}

type EvilNetSon struct {
	conn      *rudp.Conn
	localaddr string
	sonkey    string
	name      string
}

func NewEvilNet(plugins []PluginCreator, config *EvilNetConfig) *EvilNet {
	if config == nil {
		config = &EvilNetConfig{}
	}
	config.Check()

	uuid := common.UniqueId()

	ip, err := common.GetOutboundIP()
	if err != nil {
		return nil
	}
	localip := ip.String()

	ret := &EvilNet{
		config:  config,
		uuid:    uuid,
		localip: localip,
	}

	ret.plugin = make(map[string]PluginCreator)
	for _, p := range plugins {
		ret.plugin[p.Name()] = p
	}

	return ret
}

func (ev *EvilNet) Stop() {
	ev.exit = true
	ev.workResultLock.Wait()
}

func (ev *EvilNet) Run() error {

	if ev.config.ListenSonPort > 0 {
		addr := ev.localip + ":" + strconv.Itoa(ev.config.ListenSonPort)
		loggo.Info("start run son at %s", addr)

		conn, err := rudp.Listen(addr, &ev.config.Rudpconfig)
		if err != nil {
			return err
		}
		ev.son = conn

		go ev.updateSon()
	}

	addr := ev.localip + ":" + strconv.Itoa(ev.config.ListenOutPort)
	loggo.Info("start run at %s", addr)

	conn, err := rudp.Listen(addr, &ev.config.Rudpconfig)
	if err != nil {
		return err
	}
	ev.fa = conn
	ev.fa.SetNoaccept(true)

	if len(ev.config.Fatheraddr) > 0 {
		go ev.updateFather()
	} else {
		ev.globalname = ev.config.Name
	}

	return nil
}

func (ev *EvilNet) updateFather() {

	defer common.CrashLog()

	ev.workResultLock.Add(1)
	defer ev.workResultLock.Done()

	regtime := common.GetNowUpdateInSecond()

	bytes := make([]byte, 2000)

	for !ev.exit {

		needReg := false
		now := common.GetNowUpdateInSecond()

		// connect
		if ev.father == nil || !ev.father.IsConnected() {

			loggo.Info("start connect father %s", ev.config.Fatheraddr)

			conn, err := ev.fa.Dail(ev.config.Fatheraddr)
			if err != nil {
				loggo.Error("connect father fail %s", err)
				time.Sleep(time.Second)
				continue
			}
			ev.father = conn
			ef := encrypt
			df := decrypt
			ev.father.SetUserData(msgmgr.NewMsgMgr(MSG_MAX_SIZE, CONN_MSG_BUFFER_SIZE, CONN_MSG_LIST_SIZE, &ef, &df))
			needReg = true

			loggo.Info("connect father ok %s", ev.config.Fatheraddr)
		}

		// reg inter
		if now.Sub(regtime) > time.Second*time.Duration(ev.config.RegFatherInterSec) {
			needReg = true
		}

		// reg
		if needReg {
			regtime = now
			ev.regFather()
		}

		mm := ev.father.UserData().(*msgmgr.MsgMgr)

		// send msg
		packbuffer := mm.GetPackBuffer()
		n, _ := ev.father.Write(packbuffer)
		if n > 0 {
			mm.SkipPackBuffer(n)
		}

		// recv msg
		n, _ = ev.father.Read(bytes)
		if n > 0 {
			mm.WriteUnPackBuffer(bytes[0:n])
		}

		mm.Update()

		// process
		rl := mm.RecvList()
		if rl != nil {
			for e := rl.Front(); e != nil; e = e.Next() {
				rb := e.Value.([]byte)
				enm := &EvilNetMsg{}
				err := proto.Unmarshal(rb, enm)
				if err == nil {
					ev.processFather(enm)
				}
			}
		}
	}

}

func (ev *EvilNet) IsFatherConnected() bool {
	return ev.father != nil && ev.father.IsConnected()
}

func (ev *EvilNet) updateSon() {

	defer common.CrashLog()

	ev.workResultLock.Add(1)
	defer ev.workResultLock.Done()

	for !ev.exit {
		conn := ev.son.Accept(1000)
		if conn != nil {
			go ev.updateSonConn(conn)
		}
	}
}

func (ev *EvilNet) updateSonConn(conn *rudp.Conn) {

	defer common.CrashLog()

	ev.workResultLock.Add(1)
	defer ev.workResultLock.Done()

	loggo.Info("accept new son %s", conn.RemoteAddr())

	ef := encrypt
	df := decrypt
	conn.SetUserData(msgmgr.NewMsgMgr(MSG_MAX_SIZE, CONN_MSG_BUFFER_SIZE, CONN_MSG_LIST_SIZE, &ef, &df))

	bytes := make([]byte, 2000)

	for !ev.exit {
		if !conn.IsConnected() {
			break
		}

		mm := conn.UserData().(*msgmgr.MsgMgr)

		// send msg
		packbuffer := mm.GetPackBuffer()
		n, err := conn.Write(packbuffer)
		if err != nil {
			break
		}
		if n > 0 {
			mm.SkipPackBuffer(n)
		}

		// recv msg
		n, err = conn.Read(bytes)
		if err != nil {
			break
		}
		if n > 0 {
			mm.WriteUnPackBuffer(bytes[0:n])
		}

		mm.Update()

		// process
		rl := mm.RecvList()
		if rl != nil {
			for e := rl.Front(); e != nil; e = e.Next() {
				rb := e.Value.([]byte)
				enm := &EvilNetMsg{}
				err := proto.Unmarshal(rb, enm)
				if err == nil {
					ev.processSon(conn, enm)
				}
			}
		}
	}

	loggo.Info("close son %s", conn.RemoteAddr())

	conn.Close(false)
	ev.deleteSonConn(conn.Username())

	loggo.Info("close son ok %s", conn.RemoteAddr())
}

func (ev *EvilNet) addSonConn(name string, conn *EvilNetSon) {
	ev.sonConnMap.Store(name, conn)
}

func (ev *EvilNet) getSonConn(name string) *EvilNetSon {
	ret, ok := ev.sonConnMap.Load(name)
	if !ok {
		return nil
	}
	return ret.(*EvilNetSon)
}

func (ev *EvilNet) deleteSonConn(name string) {
	ev.sonConnMap.Delete(name)
}

func (ev *EvilNet) GetSonConnNum() int {
	n := 0
	ev.sonConnMap.Range(func(key, value interface{}) bool {
		n++
		return true
	})
	return n
}

func (ev *EvilNet) tryConnectPeer(addr string, mykey string, dstkey string, timeoutms int) (*rudp.Conn, bool) {

	conn, err := ev.fa.DailWithTimeout(addr, timeoutms)
	if err != nil {
		loggo.Error("connect peer fail %s -> %s", ev.fa.LocalAddr(), addr)
		return conn, false
	}

	ef := encrypt
	df := decrypt
	conn.SetUserData(msgmgr.NewMsgMgr(MSG_MAX_SIZE, CONN_MSG_BUFFER_SIZE, CONN_MSG_LIST_SIZE, &ef, &df))

	loggo.Info("connect peer ok %s %s -> %s", conn.Id(), ev.fa.LocalAddr(), addr)

	bytes := make([]byte, 2000)

	ev.SendTo(conn, []byte(dstkey))

	connected := false
	startConnectTime := time.Now()
	for !ev.exit {
		if !conn.IsConnected() {
			break
		}

		mm := conn.UserData().(*msgmgr.MsgMgr)

		// send msg
		packbuffer := mm.GetPackBuffer()
		n, err := conn.Write(packbuffer)
		if err != nil {
			break
		}
		if n > 0 {
			mm.SkipPackBuffer(n)
		}

		// recv msg
		n, err = conn.Read(bytes)
		if err != nil {
			break
		}
		if n > 0 {
			mm.WriteUnPackBuffer(bytes[0:n])
		}

		mm.Update()

		// process
		rl := mm.RecvList()
		if rl != nil {
			e := rl.Front()
			if e != nil {
				rb := e.Value.([]byte)
				if len(rb) != len(mykey) {
					break
				}
				if string(rb) != mykey {
					break
				}
				connected = true
				break
			}
		}

		// timeout
		now := time.Now()
		diffclose := now.Sub(startConnectTime)
		if diffclose > time.Millisecond*time.Duration(1000) {
			break
		}
	}

	return conn, connected
}

func (ev *EvilNet) updatePeerServer(rpcid string, plugin Plugin, localaddr string, globaladdr string, eproto string, params []string,
	mykey string, dstkey string, timeoutms int) {

	loggo.Info("start connect peer %s -> %s %s", ev.fa.LocalAddr(), localaddr, globaladdr)

	conn, connected := ev.tryConnectPeer(globaladdr, mykey, dstkey, timeoutms/2)
	if !connected {
		if conn != nil {
			conn.Close(true)
		}
		conn, connected = ev.tryConnectPeer(localaddr, mykey, dstkey, timeoutms/2)
		if !connected {
			if conn != nil {
				conn.Close(true)
			}
		}
	}

	if !connected || conn == nil {
		loggo.Error("real connect peer fail %s -> %s %s", ev.fa.LocalAddr(), localaddr, globaladdr)
		return
	}

	loggo.Info("real connect peer ok %s %s -> %s %s %s", conn.Id(), ev.fa.LocalAddr(), localaddr, globaladdr, conn.RemoteAddr())

	if len(rpcid) > 0 {
		rpc.PutRet(rpcid, ev, plugin, conn)
	}

	plugin.OnConnected(ev, conn)

	bytes := make([]byte, 2000)

	for !ev.exit && !plugin.IsClose(ev, conn) {
		if !conn.IsConnected() {
			break
		}

		mm := conn.UserData().(*msgmgr.MsgMgr)

		// send msg
		packbuffer := mm.GetPackBuffer()
		n, err := conn.Write(packbuffer)
		if err != nil {
			break
		}
		if n > 0 {
			mm.SkipPackBuffer(n)
		}

		// recv msg
		n, err = conn.Read(bytes)
		if err != nil {
			break
		}
		if n > 0 {
			mm.WriteUnPackBuffer(bytes[0:n])
		}

		mm.Update()

		// process
		rl := mm.RecvList()
		if rl != nil {
			for e := rl.Front(); e != nil; e = e.Next() {
				rb := e.Value.([]byte)
				plugin.OnRecv(ev, conn, rb)
			}
		}
	}

	loggo.Info("close son %s", conn.RemoteAddr())

	conn.Close(false)

	plugin.Close(ev, conn)
	plugin.OnClose(ev, conn)
	loggo.Info("close son ok %s", conn.RemoteAddr())
}

func (ev *EvilNet) SendTo(conn *rudp.Conn, data []byte) {
	mm := conn.UserData().(*msgmgr.MsgMgr)
	mm.Send(data)
}
