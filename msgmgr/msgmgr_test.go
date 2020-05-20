package msgmgr

import (
	"fmt"
	"testing"
)

func Test0001(t *testing.T) {
	a := NewMsgMgr(5, 14, 10, nil, nil)
	fmt.Println(a.Send([]byte("12345")))
	fmt.Println(a.Send([]byte("")))
	fmt.Println(a.Send([]byte("12345")))

	a.Update()

	fmt.Println(a.GetPackBuffer())
	a.WriteUnPackBuffer(a.GetPackBuffer())
	a.SkipPackBuffer(4)
	fmt.Println(a.GetUnPackLeftSize())
	fmt.Println(a.GetPackBuffer())

	a.Update()

	aa := a.RecvList()

	for e := aa.Front(); e != nil; e = e.Next() {
		b := e.Value.([]byte)
		fmt.Println(string(b))
	}
}
