package rudp

import (
	"fmt"
	"testing"
	"time"
)

func Test0001(t *testing.T) {

	lis, err := Listen("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis.Id())

	conn, err := Dail("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(conn.Id())

	c := lis.Accept(100)
	fmt.Println(c.Id())
	fmt.Println(c)

	c.Write([]byte("123123123123"))
	time.Sleep(time.Second)

	tmp := make([]byte, 19)
	n, err := conn.Read(tmp)
	fmt.Println(n)
	fmt.Println(string(tmp))

	conn.Close(false)
	lis.Close(false)
	fmt.Println("done ")

}

func Test0002(t *testing.T) {

	lis, err := Listen("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis.Id())

	lis1, err := Listen("127.0.0.1:8888", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis1.Id())

	lis1.SetNoaccept(true)
	conn, err := lis1.Dail("127.0.0.1:9999")
	fmt.Println(lis1.Id())

	c := lis.Accept(100)
	fmt.Println(c.Id())
	fmt.Println(c)

	c.Write([]byte("123123123123"))
	time.Sleep(time.Second)

	tmp := make([]byte, 19)
	n, err := conn.Read(tmp)
	fmt.Println(n)
	fmt.Println(string(tmp))

	conn.Close(false)
	lis.Close(false)
	fmt.Println("done ")

}

func Test0003(t *testing.T) {

	lis, err := Listen("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis.Id())

	lis1, err := Listen("127.0.0.1:8888", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis1.Id())

	lis1.SetNoaccept(true)
	conn, err := lis1.Dail("127.0.0.1:9999")
	fmt.Println(conn.Id())

	c := lis.Accept(100)
	fmt.Println(c.Id())
	fmt.Println(c)

	c.Write([]byte("123123123123"))
	time.Sleep(time.Second)

	tmp := make([]byte, 19)
	n, err := conn.Read(tmp)
	fmt.Println(n)
	fmt.Println(string(tmp))

	conn.Close(false)

	conn1, err := lis1.Dail("127.0.0.1:9999")
	fmt.Println(conn1.Id())

}

func Test0004(t *testing.T) {

	lis, err := Listen("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lis.Id())

	conn, err := Dail("127.0.0.1:9999", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(conn.Id())

	c := lis.Accept(100)
	fmt.Println(c.Id())
	fmt.Println(c)

	c.Write([]byte("123123123123"))
	time.Sleep(time.Second)

	tmp := make([]byte, 19)
	n, err := conn.Read(tmp)
	fmt.Println(n)
	fmt.Println(string(tmp))

	conn.Close(true)
	lis.Close(true)
	fmt.Println("done ")

}
