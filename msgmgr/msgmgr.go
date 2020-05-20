package msgmgr

import (
	"container/list"
	"encoding/binary"
	"errors"
	"github.com/cxjava/go-engine/rbuffergo"
	"math"
	"strconv"
)

type MsgMgr struct {
	headersize int
	buffersize int
	maxmsgsize int

	headertmp []byte

	pbuffer  *rbuffergo.RBuffergo
	upbuffer *rbuffergo.RBuffergo

	cachemsgnum int
	sendl       *list.List
	recvl       *list.List

	presend  *func([]byte) (bool, []byte)
	postrecv *func([]byte) (bool, []byte)
}

func NewMsgMgr(maxmsgsize int, buffersize int, cachemsgnum int, presend *func([]byte) (bool, []byte), postrecv *func([]byte) (bool, []byte)) *MsgMgr {
	var headersize int
	if maxmsgsize > math.MaxInt16 {
		headersize = 4
	} else if maxmsgsize > math.MaxInt8 {
		headersize = 2
	} else if maxmsgsize > 0 {
		headersize = 1
	} else {
		return nil
	}
	if maxmsgsize+headersize > buffersize {
		return nil
	}
	return &MsgMgr{
		maxmsgsize:  maxmsgsize,
		headersize:  headersize,
		buffersize:  buffersize,
		cachemsgnum: cachemsgnum,
		headertmp:   make([]byte, headersize),
		pbuffer:     rbuffergo.New(buffersize, false),
		upbuffer:    rbuffergo.New(buffersize, false),
		sendl:       list.New(),
		recvl:       list.New(),
		presend:     presend,
		postrecv:    postrecv,
	}
}

func (mp *MsgMgr) Send(data []byte) error {
	if mp.sendl.Len() >= mp.cachemsgnum {
		return errors.New("Send list full " + strconv.Itoa(mp.sendl.Len()))
	}

	if len(data) > mp.maxmsgsize {
		return errors.New("data size too big " + strconv.Itoa(len(data)))
	}

	if mp.presend != nil {
		ok, newdata := (*mp.presend)(data)
		if !ok {
			return errors.New("presend fail " + strconv.Itoa(len(data)))
		}
		data = newdata
	}

	mp.sendl.PushBack(data)
	return nil
}

func (mp *MsgMgr) RecvList() *list.List {

	if mp.recvl.Len() > 0 {
		ret := list.New()
		for e := mp.recvl.Front(); e != nil; e = e.Next() {
			data := e.Value.([]byte)
			if mp.postrecv != nil {
				ok, newdata := (*mp.postrecv)(data)
				if !ok {
					continue
				}
				data = newdata
			}
			ret.PushBack(data)
		}
		mp.recvl.Init()
		return ret
	}
	return nil
}

func (mp *MsgMgr) Update() {

	// pack
	for mp.sendl.Len() > 0 {
		e := mp.sendl.Front()
		data := e.Value.([]byte)
		if mp.pack(data) {
			mp.sendl.Remove(e)
		} else {
			break
		}
	}

	// unpack
	for mp.recvl.Len() < mp.cachemsgnum {
		data := mp.unpack()
		if data != nil {
			mp.recvl.PushBack(data)
		} else {
			break
		}
	}

}

func (mp *MsgMgr) pack(data []byte) bool {
	size := len(data)

	if size > mp.pbuffer.Capacity()-mp.pbuffer.Size() {
		return false
	}

	if mp.headersize == 4 {
		binary.LittleEndian.PutUint32(mp.headertmp, uint32(size))
	} else if mp.headersize == 2 {
		binary.LittleEndian.PutUint16(mp.headertmp, uint16(size))
	} else if mp.headersize == 1 {
		mp.headertmp[0] = (byte)(size)
	}
	mp.pbuffer.Write(mp.headertmp)
	mp.pbuffer.Write(data)
	return true
}

func (mp *MsgMgr) unpack() []byte {
	if mp.upbuffer.Size() < mp.headersize {
		return nil
	}

	mp.upbuffer.Store()
	mp.upbuffer.Read(mp.headertmp)

	var size int
	if mp.headersize == 4 {
		size = int(binary.LittleEndian.Uint32(mp.headertmp))
	} else if mp.headersize == 2 {
		size = int(binary.LittleEndian.Uint16(mp.headertmp))
	} else if mp.headersize == 1 {
		size = (int)(mp.headertmp[0])
	}

	if mp.maxmsgsize < size {
		mp.upbuffer.Restore()
		return nil
	}

	if mp.upbuffer.Size() < size {
		mp.upbuffer.Restore()
		return nil
	}

	tmp := make([]byte, size)
	mp.upbuffer.Read(tmp)

	return tmp
}

func (mp *MsgMgr) GetPackBuffer() []byte {
	return mp.pbuffer.GetReadLineBuffer()
}

func (mp *MsgMgr) SkipPackBuffer(len int) {
	mp.pbuffer.SkipRead(len)
}

func (mp *MsgMgr) GetUnPackLeftSize() int {
	return mp.upbuffer.Capacity() - mp.upbuffer.Size()
}

func (mp *MsgMgr) WriteUnPackBuffer(data []byte) {
	mp.upbuffer.Write(data)
}
