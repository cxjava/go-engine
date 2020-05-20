package evilnet

import (
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/loggo"
	"github.com/cxjava/go-engine/rpc"
	"github.com/cxjava/go-engine/rudp"
	"github.com/golang/protobuf/proto"
	"time"
)

func (ev *EvilNet) processFather(enm *EvilNetMsg) {
	loggo.Info("process father msg %s", EvilNetMsg_TYPE_name[enm.Type])

	if enm.Type == int32(EvilNetMsg_RSPREG) && enm.RspRegMsg != nil {
		ev.processFatherRspReg(enm)
	} else if enm.Type == int32(EvilNetMsg_ROUTER) && ev.father != nil && enm.RouterMsg != nil {
		ev.processRouterReg(ev.father, enm)
	}
}

func (ev *EvilNet) processFatherRspReg(enm *EvilNetMsg) {
	loggo.Info("process father rsp reg msg %s", enm.RspRegMsg.String())

	if enm.RspRegMsg.Result == "ok" {
		if enm.RspRegMsg.Sonkey == ev.config.Key {
			ev.fathername = enm.RspRegMsg.Fathername
			ev.globalname = enm.RspRegMsg.Newname
			ev.globaladdr = enm.RspRegMsg.Globaladdr
		}
	} else {
		ev.father.Close(false)
	}
}

func (ev *EvilNet) process(enm *EvilNetMsg) {
	loggo.Info("process msg %d", enm.Type)

}

func (ev *EvilNet) processSon(conn *rudp.Conn, enm *EvilNetMsg) {
	loggo.Info("process son msg %s %s", EvilNetMsg_TYPE_name[enm.Type], conn.RemoteAddr())

	if enm.Type == int32(EvilNetMsg_REQREG) && enm.ReqRegMsg != nil {
		ev.processSonReqReg(conn, enm)
	} else if enm.Type == int32(EvilNetMsg_ROUTER) && enm.RouterMsg != nil {
		ev.processRouterReg(conn, enm)
	}
}

func (ev *EvilNet) processSonReqReg(conn *rudp.Conn, enm *EvilNetMsg) {
	loggo.Info("process son req reg msg %s %s", conn.RemoteAddr(), enm.ReqRegMsg.String())

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_RSPREG)
	evm.RspRegMsg = &EvilNetRspRegMsg{}

	if enm.ReqRegMsg.Key != ev.config.Key {
		evm.RspRegMsg.Result = "key error"
	} else {
		son := ev.getSonConn(enm.ReqRegMsg.Name)
		if son != nil {
			if son.sonkey != enm.ReqRegMsg.Sonkey {
				evm.RspRegMsg.Result = "son key error"
			} else {
				if son.conn.Id() != conn.Id() {
					go func() {
						defer common.CrashLog()
						son.conn.Close(false)
					}()
					son.conn = conn
				}
				evm.RspRegMsg.Result = "ok"
			}
		} else {
			evm.RspRegMsg.Result = "ok"
		}
	}

	if evm.RspRegMsg.Result == "ok" {
		evm.RspRegMsg.Fathername = ev.config.Name
		evm.RspRegMsg.Localaddr = enm.ReqRegMsg.Localaddr
		evm.RspRegMsg.Sonkey = enm.ReqRegMsg.Sonkey
		evm.RspRegMsg.Globaladdr = conn.RemoteAddr()
		evm.RspRegMsg.Newname = ev.config.Name + "." + enm.ReqRegMsg.Name

		son := &EvilNetSon{}
		son.conn = conn
		son.localaddr = enm.ReqRegMsg.Localaddr
		son.name = enm.ReqRegMsg.Name
		son.sonkey = enm.ReqRegMsg.Sonkey

		conn.SetUsername(enm.ReqRegMsg.Name)

		ev.addSonConn(enm.ReqRegMsg.Name, son)
	}

	loggo.Info("rsp to son %s %s %s", ev.config.Fatheraddr, evm.RspRegMsg.Globaladdr, enm.ReqRegMsg.String())

	mb, _ := proto.Marshal(&evm)

	ev.SendTo(conn, mb)
}

func (ev *EvilNet) processRouterReg(conn *rudp.Conn, enm *EvilNetMsg) {
	loggo.Info("process son router msg %s %s %s", conn.RemoteAddr(), enm.RouterMsg.Src, enm.RouterMsg.Dst)

	if ev.globalname == enm.RouterMsg.Dst {

		enmr := &EvilNetMsg{}
		err := proto.Unmarshal(enm.RouterMsg.Data, enmr)
		if err != nil {
			loggo.Error("process son router msg %s %s %s %s", conn.RemoteAddr(), enm.RouterMsg.Src, enm.RouterMsg.Dst, err)
			return
		}

		loggo.Info("process son router msg %s %s %s %s", conn.RemoteAddr(), enm.RouterMsg.Src, enm.RouterMsg.Dst, EvilNetMsg_TYPE_name[enmr.Type])

		if enmr.Type == int32(EvilNetMsg_REQCONN) {
			ev.processRouterReqConn(enm.RouterMsg.Id, conn, enm.RouterMsg.Src, enm.RouterMsg.Dst, enmr)
		} else if enmr.Type == int32(EvilNetMsg_RSPCONN) {
			ev.processRouterRspConn(enm.RouterMsg.Id, conn, enm.RouterMsg.Src, enm.RouterMsg.Dst, enmr)
		} else if enmr.Type == int32(EvilNetMsg_PING) {
			ev.processRouterPing(enm.RouterMsg.Id, conn, enm.RouterMsg.Src, enm.RouterMsg.Dst, enmr)
		} else if enmr.Type == int32(EvilNetMsg_PONG) {
			ev.processRouterPong(enm.RouterMsg.Id, conn, enm.RouterMsg.Src, enm.RouterMsg.Dst, enmr)
		}

	} else {
		ev.routerMsg(enm.RouterMsg.Src, enm.RouterMsg.Dst, enm)
	}

}

func (ev *EvilNet) processRouterReqConn(rpcid string, conn *rudp.Conn, src string, dst string, enm *EvilNetMsg) {
	loggo.Info("process son router msg req conn %s", enm.ReqConnMsg.String())

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_RSPCONN)
	evm.RspConnMsg = &EvilNetRspConnMsg{}

	val, ok := ev.plugin[enm.ReqConnMsg.Proto]
	if !ok {
		evm.RspConnMsg.Result = "no proto"
	} else {
		evm.RspConnMsg.Result = "ok"
	}

	if ev.father == nil {
		evm.RspConnMsg.Result = "no father"
	}

	if evm.RspConnMsg.Result == "ok" {
		evm.RspConnMsg.Localaddr = ev.father.LocalAddr()
		evm.RspConnMsg.Globaladdr = ev.globaladdr
		evm.RspConnMsg.Proto = enm.ReqConnMsg.Proto
		evm.RspConnMsg.Key = enm.ReqConnMsg.Key
		evm.RspConnMsg.Param = enm.ReqConnMsg.Param
		evm.RspConnMsg.Randomkey = common.RandStr(16)
		evm.RspConnMsg.Timeoutms = enm.ReqConnMsg.Timeoutms

		// start connect peer
		go func() {
			defer common.CrashLog()
			ev.updatePeerServer("", val.Create(), enm.ReqConnMsg.Localaddr, enm.ReqConnMsg.Globaladdr, enm.ReqConnMsg.Proto, enm.ReqConnMsg.Param,
				evm.RspConnMsg.Randomkey, enm.ReqConnMsg.Randomkey, int(enm.ReqConnMsg.Timeoutms))
		}()
	}

	evmr := ev.packRouterMsg(rpcid, dst, src, &evm)

	mbr, _ := proto.Marshal(evmr)

	ev.SendTo(conn, mbr)
}

func (ev *EvilNet) processRouterRspConn(rpcid string, conn *rudp.Conn, src string, dst string, enm *EvilNetMsg) {
	loggo.Info("process son router msg rsp conn %s", enm.RspConnMsg.String())

	if enm.RspConnMsg.Result == "ok" {

		val, ok := ev.plugin[enm.RspConnMsg.Proto]
		if !ok {
			return
		}

		if enm.RspConnMsg.Key != ev.config.ConnectKey {
			return
		}

		// start connect peer
		go func() {
			defer common.CrashLog()
			ev.updatePeerServer(rpcid, val.Create(), enm.RspConnMsg.Localaddr, enm.RspConnMsg.Globaladdr, enm.RspConnMsg.Proto, enm.RspConnMsg.Param,
				ev.curConnRandomKey, enm.RspConnMsg.Randomkey, int(enm.RspConnMsg.Timeoutms))
		}()
	}
}

func (ev *EvilNet) processRouterPing(rpcid string, conn *rudp.Conn, src string, dst string, enm *EvilNetMsg) {
	loggo.Info("process son router msg ping %s", enm.PingMsg.String())

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_PONG)
	evm.PongMsg = &EvilNetPongMsg{}
	evm.PongMsg.Time = enm.PingMsg.Time
	var pp []string
	for n, _ := range ev.plugin {
		pp = append(pp, n)
	}
	evm.PongMsg.Proto = pp

	evmr := ev.packRouterMsg(rpcid, dst, src, &evm)

	mbr, _ := proto.Marshal(evmr)

	ev.SendTo(conn, mbr)
}

func (ev *EvilNet) processRouterPong(rpcid string, conn *rudp.Conn, src string, dst string, enm *EvilNetMsg) {
	loggo.Info("process son router msg pong %s", enm.PongMsg.String())

	now := time.Now().UnixNano()
	d := time.Duration(now - enm.PongMsg.Time)

	loggo.Info("pong from %s %s", src, d.String())

	rpc.PutRet(rpcid, d.String())
}
