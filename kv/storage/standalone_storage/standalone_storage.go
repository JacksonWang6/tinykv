package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}


type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s * StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {

	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	// fix bug: 这个判断逻辑应该放在standalone_storage.go里面,而不是server里面
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

// DBIterator也是一个接口,定义了五种方法,我们查询cf_iterator.go发现BadgerIterator实现了这五种方法,所以它是一个DBIterator
// 类型的对象,所以我们要做的就是初始化一个BadgerIterator并且返回它
func (s * StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf, s.txn)
	return it
}

func (s * StandAloneStorageReader) Close() {
	// Don’t forget to call Discard() for badger.Txn and close all iterators before discarding.
	s.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// 返回一个StandAloneStorage的实例
	database := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		db: database,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

// StorageReader是一个接口类型, 所以我们需要新建一个这个类型的结构体然后返回一个实例化的对象
// 具体来说就是创建一个实现了这个接口定义的几种方法的结构体即可
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		txn:   s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, value := range batch {
		// 根据Modify的类型来判断是put还是delete
		var err error
		switch value.Data.(type) {
		// 根据类型的不同调用不同的函数,状态机的分叉
		case storage.Put:
			err = engine_util.PutCF(s.db, value.Cf(), value.Key(), value.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.db, value.Cf(), value.Key())
		}
		// 出现了错误
		if err != nil {
			return err
		}
	}
	return nil
}
