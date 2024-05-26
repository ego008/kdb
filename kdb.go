package kdb

import (
	"bytes"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"math"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	scoreByteLen        = 8
	scoreMin     uint64 = 0
	scoreMax     uint64 = math.MaxUint64

	PrefixH  uint8 = 30 // hash prefix
	PrefixZK uint8 = 31 // Zet Key prefix
	PrefixZS uint8 = 29 // Zet Score prefix
)

type (
	DB struct {
		db        *leveldb.DB
		callCount atomic.Int32 // callCount
	}
)

// Open creates/opens a DB at specified path, and returns a DB enclosing the same.
func Open(dbPath string, o *opt.Options) (*DB, error) {
	database, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		if errors.IsCorrupted(err) {
			if database, err = leveldb.RecoverFile(dbPath, o); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &DB{db: database}, nil
}

// Close closes the DB.
func (d *DB) Close() error {
	for {
		if d.callCount.Load() == 0 {
			break
		}
		time.Sleep(4 * time.Millisecond)
	}
	return d.db.Close()
}

func (d *DB) HSet(name, key, val []byte) error {
	realKey := SCC([]byte{PrefixH, byte(len(name))}, name, key)
	d.callCount.Add(1)
	err := d.db.Put(realKey, val, nil)
	d.callCount.Add(-1)
	return err
}

// HGet return val, err
func (d *DB) HGet(name, key []byte) ([]byte, error) {
	realKey := SCC([]byte{PrefixH, byte(len(name))}, name, key)
	d.callCount.Add(1)
	val, err := d.db.Get(realKey, nil)
	d.callCount.Add(-1)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (d *DB) HMSet(name []byte, kvs [][]byte) error {
	kvLen := len(kvs)
	if kvLen == 0 || kvLen%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	keyPrefix := SCC([]byte{PrefixH, byte(len(name))}, name)
	batch := new(leveldb.Batch)
	for i := 0; i < kvLen; i += 2 {
		batch.Put(SCC(keyPrefix, kvs[i]), kvs[i+1])
	}
	d.callCount.Add(1)
	err := d.db.Write(batch, nil)
	d.callCount.Add(-1)
	return err
}

// HMGet return [][]byte [key][val][key][val]
func (d *DB) HMGet(name []byte, keys [][]byte) ([][]byte, error) {
	keysLen := len(keys)
	if keysLen == 0 {
		return [][]byte{}, errors.New("keys len must > 0")
	}
	var kvs [][]byte
	keyPrefix := SCC([]byte{PrefixH, byte(len(name))}, name)
	d.callCount.Add(1)
	for _, key := range keys {
		val, err := d.db.Get(SCC(keyPrefix, key), nil)
		if err != nil {
			continue
		}
		kvs = append(kvs, key, val)
	}
	d.callCount.Add(-1)
	return kvs, nil
}

func (d *DB) HIncr(name, key []byte, step int64) (newNum uint64, err error) {
	realKey := SCC([]byte{PrefixH, byte(len(name))}, name, key)
	var oldNum uint64
	var val []byte
	d.callCount.Add(1)
	defer d.callCount.Add(-1)
	val, err = d.db.Get(realKey, nil)
	if err == nil {
		oldNum = B2I64(val)
	}
	if step > 0 {
		if (scoreMax - uint64(step)) < oldNum {
			err = errors.New("overflow number")
			return
		}
		newNum = oldNum + uint64(step)
	} else {
		if uint64(-step) > oldNum {
			//err = errors.New("overflow number")
			//return
			newNum = 0
		} else {
			newNum = oldNum - uint64(-step)
		}
	}

	if err = d.db.Put(realKey, I2B64(newNum), nil); err != nil {
		newNum = 0
		return
	}
	return
}

func (d *DB) HGetInt(name, key []byte) (uint64, error) {
	realKey := SCC([]byte{PrefixH, byte(len(name))}, name, key)
	d.callCount.Add(1)
	val, err := d.db.Get(realKey, nil)
	d.callCount.Add(-1)
	if err != nil {
		return 0, err
	}
	return B2I64(val), nil
}

func (d *DB) HHasKey(name, key []byte) (bool, error) {
	d.callCount.Add(1)
	ok, err := d.db.Has(SCC([]byte{PrefixH, byte(len(name))}, name, key), nil)
	d.callCount.Add(-1)
	return ok, err
}

func (d *DB) HDel(name, key []byte) error {
	d.callCount.Add(1)
	err := d.db.Delete(SCC([]byte{PrefixH, byte(len(name))}, name, key), nil)
	d.callCount.Add(-1)
	return err
}

func (d *DB) HMDel(name []byte, keys [][]byte) error {
	d.callCount.Add(1)
	batch := new(leveldb.Batch)
	keyPrefix := SCC([]byte{PrefixH, byte(len(name))}, name)
	for _, key := range keys {
		batch.Delete(SCC(keyPrefix, key))
	}
	err := d.db.Write(batch, nil)
	d.callCount.Add(-1)
	return err
}

func (d *DB) HDelBucket(name []byte) error {
	d.callCount.Add(1)
	defer d.callCount.Add(-1)
	batch := new(leveldb.Batch)
	iter := d.db.NewIterator(util.BytesPrefix(SCC([]byte{PrefixH, byte(len(name))}, name)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	return d.db.Write(batch, nil)
}

// HScan return [][]byte [key][val][key][val]
func (d *DB) HScan(name, keyStart []byte, limit int) ([][]byte, error) {
	keyPrefix := SCC([]byte{PrefixH, byte(len(name))}, name)
	realKey := SCC(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)

	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}

	n := 0
	var kvs [][]byte

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	iter := d.db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			kvs = append(kvs,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return [][]byte{}, err
	}
	return kvs, nil
}

// HPrefix return [][]byte [key][val][key][val]
func (d *DB) HPrefix(name, prefix []byte, limit int) ([][]byte, error) {
	realKey := SCC([]byte{PrefixH, byte(len(name))}, name, prefix) // keyPrefix
	keyPrefixLen := len(realKey)

	sliceRange := util.BytesPrefix(realKey)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}

	n := 0
	var kvs [][]byte

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	iter := d.db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			kvs = append(kvs,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return [][]byte{}, err
	}
	return kvs, nil
}

// HRScan return [][]byte [key][val][key][val]
func (d *DB) HRScan(name, keyStart []byte, limit int) ([][]byte, error) {
	keyPrefix := SCC([]byte{PrefixH, byte(len(name))}, name)
	realKey := SCC(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)

	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Limit = realKey
	} else {
		realKey = sliceRange.Limit
	}

	n := 0
	var kvs [][]byte

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	iter := d.db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		kvs = append(kvs,
			append([]byte{}, iter.Key()[keyPrefixLen:]...),
			append([]byte{}, iter.Value()...),
		)
		n++
		if n == limit {
			break
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return [][]byte{}, err
	}
	return kvs, nil
}

func (d *DB) ZSet(name, key []byte, val uint64) error {
	score := I2B64(val)
	keyScore := SCC([]byte{PrefixZS, byte(len(name))}, name, key)           // key / score
	newScoreKey := SCC([]byte{PrefixZK, byte(len(name))}, name, score, key) // name+score+key / nil

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	oldScore, _ := d.db.Get(keyScore, nil)
	if !bytes.Equal(oldScore, score) {
		batch := new(leveldb.Batch)
		batch.Put(keyScore, score)
		batch.Put(newScoreKey, nil)
		batch.Delete(SCC([]byte{PrefixZK, byte(len(name))}, name, oldScore, key))
		return d.db.Write(batch, nil)
	}
	return nil
}

func (d *DB) ZIncr(name, key []byte, step int64) (uint64, error) {
	keyScore := SCC([]byte{PrefixZS, byte(len(name))}, name, key) // key / score

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	score := d.ZGet(name, key) // get old score
	oldScoreB := I2B64(score)  // old score byte
	if step > 0 {
		if (scoreMax - uint64(step)) < score {
			return 0, errors.New("overflow number")
		}
		score += uint64(step)
	} else {
		if uint64(-step) > score {
			return 0, errors.New("overflow number")
		}
		score -= uint64(-step)
	}

	newScoreB := I2B64(score)

	batch := new(leveldb.Batch)
	batch.Put(keyScore, newScoreB)
	batch.Put(SCC([]byte{PrefixZS, byte(len(name))}, name, newScoreB, key), nil)
	batch.Delete(SCC([]byte{PrefixZS, byte(len(name))}, name, oldScoreB, key))
	err := d.db.Write(batch, nil)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (d *DB) ZGet(name, key []byte) uint64 {
	d.callCount.Add(1)
	val, err := d.db.Get(SCC([]byte{PrefixZS, byte(len(name))}, name, key), nil)
	d.callCount.Add(-1)
	if err != nil {
		return 0
	}
	return B2I64(val)
}

func (d *DB) ZHasKey(name, key []byte) (bool, error) {
	d.callCount.Add(1)
	ok, err := d.db.Has(SCC([]byte{PrefixZS, byte(len(name))}, name, key), nil)
	d.callCount.Add(-1)
	return ok, err
}

func (d *DB) ZDel(name, key []byte) error {
	keyScore := SCC([]byte{PrefixZS, byte(len(name))}, name, key) // key / score

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	oldScore, err := d.db.Get(keyScore, nil)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Delete(keyScore)
	batch.Delete(SCC([]byte{PrefixZK, byte(len(name))}, name, oldScore, key))
	return d.db.Write(batch, nil)
}

func (d *DB) ZDelBucket(name []byte) error {
	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	batch := new(leveldb.Batch)

	iter := d.db.NewIterator(util.BytesPrefix(SCC([]byte{PrefixZS, byte(len(name))}, name)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	iter = d.db.NewIterator(util.BytesPrefix(SCC([]byte{PrefixZK, byte(len(name))}, name)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}

	return d.db.Write(batch, nil)
}

func (d *DB) ZMSet(name []byte, kvs [][]byte) error {
	kvsLen := len(kvs)
	if kvsLen == 0 || kvsLen%2 != 0 {
		return errors.New("kvs len must is an even number")
	}

	keyPrefix1 := SCC([]byte{PrefixZS, byte(len(name))}, name)
	keyPrefix2 := SCC([]byte{PrefixZK, byte(len(name))}, name)

	d.callCount.Add(1)

	batch := new(leveldb.Batch)
	for i := 0; i < kvsLen; i += 2 {
		key, score := kvs[i], kvs[i+1]

		keyScore := SCC(keyPrefix1, key)           // key / score
		newScoreKey := SCC(keyPrefix2, score, key) // name+score+key / nil

		oldScore, _ := d.db.Get(keyScore, nil)
		if !bytes.Equal(oldScore, score) {
			batch.Put(keyScore, score)
			batch.Put(newScoreKey, nil)
			batch.Delete(SCC(keyPrefix2, oldScore, key))
		}
	}
	err := d.db.Write(batch, nil)
	d.callCount.Add(-1)
	return err
}

// ZMGet return [][]byte [key][val/score][key][val/score]
func (d *DB) ZMGet(name []byte, keys [][]byte) ([][]byte, error) {
	keyPrefix := SCC([]byte{PrefixZS, byte(len(name))}, name)
	var n int

	var kvs [][]byte

	d.callCount.Add(1)
	for _, key := range keys {
		val, err := d.db.Get(SCC(keyPrefix, key), nil)
		if err != nil {
			continue
		}
		kvs = append(kvs, key, val)
		n++
	}
	d.callCount.Add(-1)

	return kvs, nil
}

func (d *DB) ZMDel(name []byte, keys [][]byte) error {
	batch := new(leveldb.Batch)
	keyPrefix := SCC([]byte{PrefixZS, byte(len(name))}, name)
	keyPrefix2 := SCC([]byte{PrefixZK, byte(len(name))}, name)
	d.callCount.Add(1)
	for _, key := range keys {
		keyScore := SCC(keyPrefix, key) // key / score
		oldScore, err := d.db.Get(keyScore, nil)
		if err != nil {
			continue
		}
		batch.Delete(keyScore)
		batch.Delete(SCC(keyPrefix2, oldScore, key))
	}
	err := d.db.Write(batch, nil)
	d.callCount.Add(-1)
	return err
}

// ZScan return [][]byte [key][val/score][key][val/score]
func (d *DB) ZScan(name, keyStart, scoreStart []byte, limit int) ([][]byte, error) {
	if len(scoreStart) == 0 {
		scoreStart = I2B64(scoreMin)
	}

	keyPrefix := SCC([]byte{PrefixZK, byte(len(name))}, name)
	realKey := SCC(keyPrefix, scoreStart, keyStart)

	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen

	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(SCC(keyPrefix, scoreStart)).Limit
	}
	sliceRange.Start = realKey

	n := 0
	var kvs [][]byte

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	iter := d.db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			kvs = append(kvs,
				append([]byte{}, iter.Key()[scoreEndIndex:]...),
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return [][]byte{}, err
	}
	return kvs, nil
}

// ZRScan return [][]byte [key][val/score][key][val/score]
func (d *DB) ZRScan(name, keyStart, scoreStart []byte, limit int) ([][]byte, error) {
	if len(scoreStart) == 0 {
		scoreStart = I2B64(scoreMax)
	}

	keyPrefix := SCC([]byte{PrefixZK, byte(len(name))}, name)
	realKey := SCC(keyPrefix, scoreStart, keyStart)
	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen

	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(SCC(keyPrefix, scoreStart)).Start
	}
	sliceRange.Limit = realKey

	n := 0
	var kvs [][]byte

	d.callCount.Add(1)
	defer d.callCount.Add(-1)

	iter := d.db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if bytes.Compare(realKey, iter.Key()) == 1 {
			kvs = append(kvs,
				append([]byte{}, iter.Key()[scoreEndIndex:]...),
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		return [][]byte{}, err
	}
	return kvs, nil
}

// stringHeader instead of reflect.StringHeader
type stringHeader struct {
	data unsafe.Pointer
	len  int
}

// sliceHeader instead of reflect.SliceHeader
type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func S2B(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&sliceHeader{
		data: (*stringHeader)(unsafe.Pointer(&s)).data,
		len:  len(s),
		cap:  len(s),
	}))
}

func B2S(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func I2B32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func B2I32(v []byte) uint32 {
	return binary.BigEndian.Uint32(v)
}

func I2B64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func B2I64(v []byte) uint64 {
	if len(v) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v[:8])
}

// SCC concat a list of byte/string
func SCC[T any](ss ...[]T) []T {
	slicesLen := len(ss)
	var totalLen int

	for i := 0; i < slicesLen; i++ {
		totalLen += len(ss[i])
	}

	result := make([]T, totalLen)

	var j int
	for i := 0; i < slicesLen; i++ {
		j += copy(result[j:], ss[i])
	}

	return result
}

func KvEach(bss [][]byte, fn func(k, v []byte)) int {
	for i := 0; i < len(bss); i += 2 {
		fn(bss[i], bss[i+1])
	}
	return len(bss) / 2
}
