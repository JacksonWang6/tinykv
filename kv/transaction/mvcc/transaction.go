package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	var value []byte
	if write != nil {
		value = write.ToBytes()
	}
	put := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, ts),
			Value: value,
			Cf:    engine_util.CfWrite,
		},
	}
	txn.writes = append(txn.writes, put)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	// when i do 4B, it happens alot of bugs because of 4a lack of essential error check and lead to *nil err
	get, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil || get == nil {
		return nil, err
	}
	getLock, err := ParseLock(get)
	if err != nil {
		return nil, err
	}
	return getLock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, put)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	delete := storage.Modify{
		Data: storage.Delete{
			Key:   key,
			Cf:    engine_util.CfLock,
		},
	}
	txn.writes = append(txn.writes, delete)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	itor := txn.Reader.IterCF(engine_util.CfWrite)
	defer itor.Close()
	// aho, 做4B的时候发现自己对于seek函数的理解有很大的问题哇:
	// 定位到第一个大于等于这个 Key-Version 的位置, 如果不存在这个key-version, 那么会定位到比它大的最小的那个
	// 经过询问大佬, 终于搞懂了这个seek函数了, key的格式是这样的 keyN1-versionN2 -> value
	// 按照ukey升ts降，seek是按照整体排序seek的
	// 	Key1-Version3 -> Value
	//	Key1-Version2 -> Value
	//	Key1-Version1 -> Value
	//	……
	//	Key2-Version4 -> Value
	//	Key2-Version3 -> Value
	//	Key2-Version2 -> Value
	//	Key2-Version1 -> Value
	//	……
	//	KeyN-Version2 -> Value
	//	KeyN-Version1 -> Value
	//	……
	// 具体来说就是如果key不存在, 那么就seek大的最小的那个, 如果key存在, 但是version不存在, 那么就seek到比version小的最大的那个
	itor.Seek(EncodeKey(key, txn.StartTS))
	for itor.Valid() {
		k := itor.Item().Key()
		// maybe absent
		if !bytes.Equal(key, DecodeUserKey(k)) {
			return nil, nil
		}
		value, err := itor.Item().Value()
		if err != nil {
			return nil, err
		}
		if value != nil {
			// cf write列族里面保存的是changes, 保存在一个write结构体里面, 所以我们解析值解析出来的是一个write结构体
			write, err := ParseWrite(value)
			if err != nil || write == nil {
				return nil, err
			}
			if write.StartTS > txn.StartTS {
				// 如果提交的时间比我们的st大, 那么我们不能采用这个值
				itor.Next()
				continue
			}
			// write里面存储了这个版本的key在cf_default里面的ts
			return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
		}
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	put := storage.Modify{
		Data: storage.Put{
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
			Cf:    engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, put)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	delete := storage.Modify{
		Data: storage.Delete{
			Key:   EncodeKey(key, txn.StartTS),
			Cf:    engine_util.CfDefault,
		},
	}
	txn.writes = append(txn.writes, delete)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	itor := txn.Reader.IterCF(engine_util.CfWrite)
	defer itor.Close()
	itor.Seek(EncodeKey(key, TsMax))
	for itor.Valid() {
		k := itor.Item().Key()
		if !bytes.Equal(key, DecodeUserKey(k)) {
			return nil, 0, nil
		}
		value , err := itor.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(k), nil
		}
		itor.Next()
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	itor := txn.Reader.IterCF(engine_util.CfWrite)
	defer itor.Close()
	itor.Seek(EncodeKey(key, TsMax))
	if itor.Valid() {
		k := itor.Item().Key()
		if !bytes.Equal(key, DecodeUserKey(k)) {
			return nil, 0, nil
		}
		value , err := itor.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		return write, decodeTimestamp(k), nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
