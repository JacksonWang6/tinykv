package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn *MvccTxn
	iter engine_util.DBIterator
	nextKey []byte
}

// 这个就是在rawScan上面的一层封装, 因为底层数据库一个键有好多版本, 我们只需要满足当前ts的最新版本即可
// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	// 判断是否遍历完了
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	// 定位到下一个key的位置
	scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	log.Info("%v %v", scan.iter.Item().Key(), DecodeUserKey(scan.iter.Item().Key()))
	// 判断这个key是否符合要求, 不符合的话持续向前iter
	// fix bug: 添加了ts的判断
	for !bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), scan.nextKey) ||
		decodeTimestamp(scan.iter.Item().Key()) >= scan.txn.StartTS {
		scan.nextKey = DecodeUserKey(scan.iter.Item().Key())
		scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
	}
	item := scan.iter.Item()
	key := DecodeUserKey(item.Key())
	wvalue, err := item.Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(wvalue)
	if err != nil {
		return nil, nil, err
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	if write.Kind != WriteKindPut {
		// TestScanDeleted4C: 这个测试里面存在scan到writeKind不为put的情况, 我们得跳过这种情况
		value = nil
	}
	// 更新nextkey
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		if !bytes.Equal(key, DecodeUserKey(scan.iter.Item().Key())) {
			scan.nextKey = DecodeUserKey(scan.iter.Item().Key())
			break
		}
	}
	return key, value, nil
}
