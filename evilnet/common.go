package evilnet

import (
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/rudp"
	"math"
)

const (
	MSG_MAX_SIZE         int    = math.MaxInt16
	CONN_MSG_BUFFER_SIZE int    = 100 * 1024
	CONN_MSG_LIST_SIZE   int    = 100
	MSG_RC4_KEY          string = "deadbeef"
)

func encrypt(data []byte) (bool, []byte) {
	newdata, err := common.Rc4(MSG_RC4_KEY, data)
	if err != nil {
		return false, nil
	}
	return true, newdata
}

func decrypt(data []byte) (bool, []byte) {
	newdata, err := common.Rc4(MSG_RC4_KEY, data)
	if err != nil {
		return false, nil
	}
	return true, newdata
}

type PluginCreator interface {
	Name() string
	Create() Plugin
}

type Plugin interface {
	Close(ev *EvilNet, conn *rudp.Conn)
	IsClose(ev *EvilNet, conn *rudp.Conn) bool

	OnConnected(ev *EvilNet, conn *rudp.Conn)
	OnRecv(ev *EvilNet, conn *rudp.Conn, data []byte)
	OnClose(ev *EvilNet, conn *rudp.Conn)
}
