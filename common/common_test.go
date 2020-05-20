package common

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func elapsed() {
	defer Elapsed(func(d time.Duration) {
		fmt.Println("use time " + d.String())
	})()

	time.Sleep(time.Second)
}

func Test0001(t *testing.T) {

	a := RandStr(5)
	a1 := RandStr(5)
	fmt.Println(a)
	fmt.Println(a1)

	fmt.Println(GetOutboundIP())

	fmt.Println(GetNowUpdateInSecond())

	d, _ := Rc4("123456", []byte("asdgdsagdsag435t43321dsgesg"))
	fmt.Println(string(d))

	d, _ = Rc4("123456", d)
	fmt.Println(string(d))

	dd := MAKEINT64(12345, 7890)
	fmt.Println(dd)
	fmt.Println(HIINT32(dd))
	fmt.Println(LOINT32(dd))
	ddd := MAKEINT32(12345, 7890)
	fmt.Println(ddd)
	fmt.Println(HIINT16(ddd))
	fmt.Println(LOINT16(ddd))

	fmt.Println(IsInt(3.0002))
	fmt.Println(IsInt(3))
	fmt.Println(strconv.FormatFloat(3.1415, 'E', -1, 64))

	aa := []int{1, 2, 3, 4, 5, 6, 7, 8}
	Shuffle(len(aa), func(i, j int) { aa[i], aa[j] = aa[j], aa[i] })
	fmt.Println(aa)

	fmt.Println(RandInt())
	fmt.Println(RandInt31n(10))

	fmt.Println(WrapString("abc", 10))

	ts := StrTable{}
	ts.AddHeader("a")
	ts.AddHeader("b")
	ts.AddHeader("c")
	tsl := StrTableLine{}
	tsl.AddData("1234")
	tsl.AddData("123421412")
	ts.AddLine(tsl)
	tsl = StrTableLine{}
	tsl.AddData("aaa")
	ts.AddLine(tsl)
	fmt.Println(WrapString("abc", 10))
	fmt.Println(ts.String("\t"))

	elapsed()
}

type TestStruct struct {
	A int
	B int64
	C string
}

func Test0002(t *testing.T) {
	ts := TestStruct{1, 2, "3"}
	st := StrTable{}
	st.AddHeader("AA")
	st.FromStruct(&ts, func(name string) bool {
		return name != "A"
	})
	stl := StrTableLine{}
	stl.AddData("a")
	stl.FromStruct(&st, &ts, func(name string, v interface{}) interface{} {
		if name == "B" {
			return time.Duration(v.(int64)).String()
		}
		return v
	})
	st.AddLine(stl)
	ts = TestStruct{12, 214124, "124123"}
	stl = StrTableLine{}
	stl.AddData("aaa")
	stl.FromStruct(&st, &ts, func(name string, v interface{}) interface{} {
		if name == "B" {
			return time.Duration(v.(int64)).String()
		}
		return v
	})
	st.AddLine(stl)
	fmt.Println(st.String(""))

	SaveJson("test.json", &ts)
	ts1 := TestStruct{}
	err := LoadJson("test.json", &ts1)
	fmt.Println(err)
	fmt.Println(ts1.C)
}

func Test0003(t *testing.T) {
	a := NumToHex(12345745643, LITTLE_LETTERS)
	b := NumToHex(12345745643, FULL_LETTERS)
	fmt.Println(a)
	fmt.Println(b)
	fmt.Println(Hex2Num(a, LITTLE_LETTERS))
	fmt.Println(Hex2Num(b, FULL_LETTERS))
	aa := NumToHex(37, LITTLE_LETTERS)
	bb := NumToHex(37, FULL_LETTERS)
	fmt.Println(aa)
	fmt.Println(bb)
	cc := Hex2Num("1i39pJZR", FULL_LETTERS)
	fmt.Println(cc)
	fmt.Println(NumToHex(cc, FULL_LETTERS))
	fmt.Println(NumToHex(cc+1, FULL_LETTERS))

	dd := Hex2Num("ZZZZZZZZ", FULL_LETTERS)
	fmt.Println(dd)
	fmt.Println(NumToHex(dd, FULL_LETTERS))
}

type TestStruct1 struct {
	TestStruct
	D int64
}

func Test0004(t *testing.T) {
	ts := TestStruct{1, 2, "3"}
	ts1 := TestStruct1{ts, 3}
	fmt.Println(StructToTable(&ts1))
}
