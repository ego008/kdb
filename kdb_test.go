package kdb

import (
	"bytes"
	"os"
	"testing"
)

func bssEqual(bss1, bss2 [][]byte) bool {
	if len(bss1) != len(bss2) {
		return false
	}
	for i := 0; i < len(bss1); i++ {
		if bytes.Compare(bss1[i], bss2[i]) != 0 {
			return false
		}
	}
	return true
}

func TestYdb(t *testing.T) {
	// Open
	db, err := Open("db", nil)
	if err != nil {
		t.Errorf("db open err, %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll("db")
	}()

	name := []byte("n")
	k := []byte("k")
	v := []byte("v")

	// HSet
	if err = db.HSet(name, k, v); err != nil {
		t.Errorf("HSet err %v", err)
	}
	var bs []byte
	var bss [][]byte
	// HGet
	bs, err = db.HGet(name, k)
	if err != nil {
		t.Errorf("HGet err %v", err)
	}
	want := v
	got := bs
	if !bytes.Equal(got, want) {
		t.Errorf("HGet got %q, wanted %q", got, want)
	}
	// HMSet
	k2 := []byte("k2")
	v2 := []byte("v2")
	if err = db.HMSet(name, [][]byte{k, v, k2, v2}); err != nil {
		t.Errorf("HMSet err %v", err)
	}
	// HMGet
	bss, err = db.HMGet(name, [][]byte{k, k2})
	if err != nil {
		t.Errorf("HMGet err %v", err)
	}
	wants := [][]byte{k, v, k2, v2}
	if !bssEqual(bss, wants) {
		t.Errorf("HMGet got %q, wanted %q", bss, wants)
	}

	// empty value
	k4 := []byte("k4")
	_ = db.HSet(name, k4, nil)
	bss, err = db.HMGet(name, [][]byte{k, k4})
	if err != nil {
		t.Errorf("HMGet err %v", err)
	}
	wants = [][]byte{k, v, k4, nil}
	if !bssEqual(bss, wants) {
		t.Errorf("HMGet got %q, wanted %q", bss, wants)
	}
	//
	var n uint64
	var k3 = []byte("k3")
	n, err = db.HIncr(name, k3, 2)
	if err != nil {
		t.Errorf("HIncr new key err %v", err)
	}
	if n != 2 {
		t.Errorf("HIncr err %v", err)
	}
	n, err = db.HIncr(name, k3, -1)
	if err != nil {
		t.Errorf("HIncr err %v", err)
	}
	if n != 1 {
		t.Errorf("HIncr negative integer err %v", err)
	}
	n, err = db.HIncr(name, k3, 9)
	if err != nil {
		t.Errorf("HIncr err %v", err)
	}
	if n != 10 {
		t.Errorf("HIncr err %v", n)
	}
	// HGetInt
	n, err = db.HGetInt(name, k3)
	if err != nil {
		t.Errorf("HGetInt err %v", err)
	}
	if n != 10 {
		t.Errorf("HGetInt err %v", n)
	}
	// HHasKey
	var ok bool
	ok, err = db.HHasKey(name, k3)
	if err != nil {
		t.Errorf("HHasKey err %v", err)
	}
	if !ok {
		t.Errorf("HHasKey err %v", ok)
	}
	// HDel
	err = db.HDel(name, k3)
	if err != nil {
		t.Errorf("HDel err %v", err)
	}
	ok, err = db.HHasKey(name, k3)
	if err != nil {
		t.Errorf("HHasKey err %v", err)
	}
	if ok {
		t.Errorf("HDel ok %v", ok)
	}
	// HMDel
	err = db.HMDel(name, [][]byte{k, k2})
	if err != nil {
		t.Errorf("HMDel err %v", err)
	}
	bss, err = db.HMGet(name, [][]byte{k, k2})
	if err != nil {
		t.Errorf("HMGet err %v", err)
	}

	//HDelBucket
	_ = db.HMSet(name, [][]byte{k, v, k2, v2})
	err = db.HDelBucket(name)
	if bss, err = db.HMGet(name, [][]byte{k, k2}); err != nil {
		t.Errorf("HMGet err %v", err)
	}
	if len(bss) != 0 {
		t.Errorf("HDelBucket false")
	}

	// HScan
	v3 := []byte("v3")
	k4, v4 := []byte("k4"), []byte("v4")
	name2 := []byte("n2")
	_ = db.HMSet(name, [][]byte{k, v, k2, v2, k3, v3, k4, v4})
	_ = db.HMSet(name2, [][]byte{k, v, k2, v2, k3, v3, k4, v4})
	if bss, err = db.HScan(name, k3, 2); err != nil {
		t.Errorf("HScan err %v", err)
	}
	wants = [][]byte{k4, v4}
	if !bssEqual(bss, wants) {
		t.Errorf("HScan got %q, wanted %q", bss, wants)
	}

	// HRScan
	if bss, err = db.HRScan(name, k2, 2); err != nil {
		t.Errorf("HRScan err %v", err)
	}
	wants = [][]byte{k, v}
	if !bssEqual(bss, wants) {
		t.Errorf("HRScan got %q, wanted %q", bss, wants)
	}

	// ZSet
	if err = db.ZSet(name, k, 1); err != nil {
		t.Errorf("ZSet err %v", err)
	}
	// ZIncr
	if n, err = db.ZIncr(name, k, 1); err != nil {
		t.Errorf("ZIncr err %v", err)
	}
	if n != 2 {
		t.Errorf("ZIncr false %d", n)
	}
	if n, err = db.ZIncr(name, k, -1); err != nil {
		t.Errorf("ZIncr err %v", err)
	}
	if n != 1 {
		t.Errorf("ZIncr false %d", n)
	}

	// ZGet
	n = db.ZGet(name, k)
	if n != 1 {
		t.Errorf("ZGet false %d", n)
	}

	// ZHasKey
	if ok, _ = db.ZHasKey(name, k); !ok {
		t.Errorf("ZHasKey false %v", ok)
	}
	if ok, _ = db.ZHasKey(name, k2); ok {
		t.Errorf("ZHasKey false %v", ok)
	}

	// ZDel
	_ = db.ZDel(name, k)
	if ok, _ = db.ZHasKey(name, k); ok {
		t.Errorf("ZDel false %v", ok)
	}

	// ZDelBucket
	_ = db.ZSet(name, k, 1)
	_ = db.ZDelBucket(name)
	if ok, _ = db.ZHasKey(name, k); ok {
		t.Errorf("ZDelBucket false %v", ok)
	}

	// ZMSet
	sv := I2B64(1)
	_ = db.ZMSet(name, [][]byte{k, sv, k2, sv})
	if ok, _ = db.ZHasKey(name, k); !ok {
		t.Errorf("ZMSet false %v", ok)
	}
	if ok, _ = db.ZHasKey(name, k2); !ok {
		t.Errorf("ZMSet false %v", ok)
	}
	if ok, _ = db.ZHasKey(name, k3); ok {
		t.Errorf("ZMSet false %v", ok)
	}

	// ZMGet
	if bss, err = db.ZMGet(name, [][]byte{k, k2}); err != nil {
		t.Errorf("ZMGet false %v", err)
	}
	wants = [][]byte{k, sv, k2, sv}
	if !bssEqual(bss, wants) {
		t.Errorf("ZMGet got %q, wanted %q", bss, wants)
	}

	// empty val
	_ = db.ZSet(name, k4, 0)
	if bss, err = db.ZMGet(name, [][]byte{k, k4}); err != nil {
		t.Errorf("ZMGet false %v", err)
	}
	wants = [][]byte{k, sv, k4, I2B64(0)}
	if !bssEqual(bss, wants) {
		t.Errorf("ZMGet got %q, wanted %q", bss, wants)
	}

	// ZMDel
	_ = db.ZMDel(name, [][]byte{k, k2})
	if bss, err = db.ZMGet(name, [][]byte{k, k2}); len(bss) != 0 {
		t.Errorf("ZMDel false %v", ok)
	}

	// ZScan
	_ = db.ZMSet(name, [][]byte{k, sv, k2, sv, k3, sv})
	if bss, err = db.ZScan(name, k, nil, 1); len(bss) == 0 {
		t.Errorf("ZScan false %d", len(bss))
	}
	if bss, _ = db.ZScan(name, k3, sv, 1); len(bss) > 0 {
		t.Errorf("ZScan false %d", len(bss))
	}
	// ZRScan
	if bss, _ = db.ZRScan(name, k3, nil, 1); len(bss) == 0 {
		t.Errorf("ZRScan false %d", len(bss))
	}
	if bss, _ = db.ZRScan(name, k4, I2B64(0), 1); len(bss) > 0 {
		t.Errorf("ZRScan false %d", len(bss))
	}

	// latest callCount
	gotC := db.callCount.Load()
	if gotC != 0 {
		t.Errorf("db callCount err, want: 0, got: %d", gotC)
	}
}
