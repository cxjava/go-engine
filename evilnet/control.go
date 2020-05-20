package evilnet

import (
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/loggo"
	"github.com/golang/protobuf/proto"
	"strings"
	"time"
)

func (ev *EvilNet) regFather() {

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_REQREG)
	evm.ReqRegMsg = &EvilNetReqRegMsg{}
	evm.ReqRegMsg.Name = ev.config.Name
	evm.ReqRegMsg.Key = ev.config.FatherKey
	evm.ReqRegMsg.Sonkey = ev.config.Key
	evm.ReqRegMsg.Localaddr = ev.father.LocalAddr()

	loggo.Info("reg to father %s %s", evm.ReqRegMsg.Localaddr, ev.config.Fatheraddr)

	mb, _ := proto.Marshal(&evm)

	ev.SendTo(ev.father, mb)
}

func (ev *EvilNet) Connect(rpcid string, dst string, eproto string, param []string, timeoutms int) {

	if ev.father == nil {
		loggo.Info("Connect fail no father %s %s", dst, ev.config.Fatheraddr)
		return
	}

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_REQCONN)
	evm.ReqConnMsg = &EvilNetReqConnMsg{}
	evm.ReqConnMsg.Key = ev.config.ConnectKey
	evm.ReqConnMsg.Proto = eproto
	evm.ReqConnMsg.Localaddr = ev.father.LocalAddr()
	evm.ReqConnMsg.Globaladdr = ev.globaladdr
	evm.ReqConnMsg.Param = param
	evm.ReqConnMsg.Randomkey = common.RandStr(16)
	evm.ReqConnMsg.Timeoutms = int32(timeoutms)

	evmr := ev.packRouterMsg(rpcid, ev.globalname, dst, &evm)

	ev.routerMsg(ev.globalname, dst, evmr)

	ev.curConnRandomKey = evm.ReqConnMsg.Randomkey

	loggo.Info("try connect to %s by father %s", dst, ev.config.Fatheraddr)
}

func (ev *EvilNet) packRouterMsg(rpcid string, src string, dst string, enm *EvilNetMsg) *EvilNetMsg {

	mb, _ := proto.Marshal(enm)

	evmr := EvilNetMsg{}
	evmr.Type = int32(EvilNetMsg_ROUTER)
	evmr.RouterMsg = &EvilNetRouterMsg{}
	evmr.RouterMsg.Src = src
	evmr.RouterMsg.Dst = dst
	evmr.RouterMsg.Data = mb
	evmr.RouterMsg.Id = rpcid

	return &evmr
}

func (ev *EvilNet) routerMsg(src string, dst string, enm *EvilNetMsg) {

	if dst == ev.globalname {
		loggo.Error("router self ignore %s", dst)
		return
	}

	dsts := strings.Split(dst, ".")
	curs := strings.Split(ev.globalname, ".")

	// dst A.B.C.D.E.F, cur A.B
	if strings.Contains(dst+".", ev.globalname+".") && len(dsts) > len(curs) {
		sonname := dsts[len(curs)]
		son := ev.getSonConn(sonname)
		if son != nil {
			mb, _ := proto.Marshal(enm)
			ev.SendTo(son.conn, mb)

			loggo.Info("%s router %s->%s msg to son %s", ev.globalname, src, dst, son.name)
			return
		} else {
			loggo.Error("%s router %s->%s msg no son", ev.globalname, src, dst)
			return
		}
	}

	// dst A.D.E.F, cur A.B
	// dst A.B.E.F, cur A.B.C
	// dst A.D.E.F, cur A.B.C
	if dsts[0] == curs[0] {
		if ev.father != nil && ev.father.IsConnected() {
			mb, _ := proto.Marshal(enm)
			ev.SendTo(ev.father, mb)

			loggo.Info("router %s->%s msg to father %s %s", src, dst, ev.fathername, ev.globalname)
			return
		} else {
			loggo.Error("router %s->%s msg no father %s %s", src, dst, ev.fathername, ev.globalname)
			return
		}
	}

	loggo.Error("router %s->%s msg ignore %s", src, dst, ev.globalname)
}

func (ev *EvilNet) Ping(rpcid string, dst string) {

	evm := EvilNetMsg{}
	evm.Type = int32(EvilNetMsg_PING)
	evm.PingMsg = &EvilNetPingMsg{}
	evm.PingMsg.Time = time.Now().UnixNano()

	evmr := ev.packRouterMsg(rpcid, ev.globalname, dst, &evm)

	ev.routerMsg(ev.globalname, dst, evmr)

	loggo.Info("send ping %s", dst)
}
