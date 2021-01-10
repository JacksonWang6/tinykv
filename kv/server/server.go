package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	value, err :=  reader.GetCF(req.Cf, req.Key)
	// 如果键没有找到,那么按照第二个测试,我们应该返回
	if value == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	// 找到了, 那就直接返回值就可以了
	return &kvrpcpb.RawGetResponse{
		Value:                value,
	}, err
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Modify代表操作,有两种,一种是PUT,一种是Delete
	return nil, server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	return nil, server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Key:   req.Key,
				Cf:    req.Cf,
			},
		},
	})
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// 首先拿到reader
	reader, _ := server.storage.Reader(nil)
	// 然后拿到迭代器
	it := reader.IterCF(req.Cf)
	it.Seek(req.StartKey)
	var i uint32
	res := kvrpcpb.RawScanResponse{
		Kvs: []*kvrpcpb.KvPair{},
	}
	// 遍历迭代器
	for i = 0; i < req.Limit; i++ {
		item := it.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		it.Next()
		if it.Valid() == false {
			break
		}
	}
	//! 关闭迭代器
	it.Close()
	return &res, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	// Any request might cause a region error, these should be handled in the same way as for the raw requests.
	// ref: Coprocessor func
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, txn.StartTS, resp) {
		// has been locked by another KvGet
		resp.Error = new(kvrpcpb.KeyError)
		resp.Error.Locked = lock.Info(req.Key)
		return resp, nil
	}
	// now get value use mvcc api
	// the doc say that  Otherwise, TinyKV must search the versions of the key to find the most recent,
	// valid value.
	// but i think the most recent may be wrong according to the tests, so i directed getValue
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// preWrite + commit 构成了 二阶段提交的主要逻辑

// preWrite，即将此事务涉及写入的所有 key 上锁并写入 value。
// 在 prewrite 时，我们用 Mutation 来表示每一个 key 的写入。Mutation 分为 Put，Delete，Lock 和 Insert 四种类型。
// Put 即对该 key 写入一个 value，Delete 即删除这个 key。Insert 与 Put 的区别是，它在执行时会检查该 key 是否存在，
// 仅当该 key 不存在时才会成功写入。Lock 是一种特殊的写入，并不是 Percolator 模型中的 Lock，它对数据不进行实际更改，
// 当一个事务读了一些 key、写了另一些 key 时，如果需要确保该事务成功提交时这些 key 不会发生改变，那么便应当对这些读到的 key
// 写入这个 Lock 类型的 Mutation。
// 上面是官方博客里面对tikv prewrite的介绍, tinykv还是有一些不同的
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if len(req.Mutations) == 0 {
		return resp, nil
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// To avoid race conditions, you can latch any key in the database
	var keysToLatch [][]byte
	for _, muta := range req.Mutations {
		keysToLatch = append(keysToLatch, muta.Key)
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)
	for _, muta := range req.Mutations {
		lock, err := txn.GetLock(muta.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts < txn.StartTS {
			// two prewrites to the same key causes a lock error
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked:               lock.Info(muta.Key),
			})
			continue
		}
		// check conflict, by get MostRecentWrite and contrast commit ts with txn'st
		write, commitTs, err := txn.MostRecentWrite(muta.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil && commitTs >= txn.StartTS {
			// conflict
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict:             &kvrpcpb.WriteConflict{
					StartTs:              txn.StartTS,
					ConflictTs:           commitTs,
					Key:                  muta.Key,
					Primary:              req.PrimaryLock,
				},
			})
			continue
		}
		switch muta.Op {
		case kvrpcpb.Op_Put:
			txn.PutLock(muta.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
			txn.PutValue(muta.Key, muta.Value)
		case kvrpcpb.Op_Del:
			txn.PutLock(muta.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
			txn.DeleteValue(muta.Key)
		case kvrpcpb.Op_Rollback:
		case kvrpcpb.Op_Lock:
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 把写在 CF_LOCK 中的锁删掉，用 commit_ts 在 CF_WRITE 写入事务提交的记录。
// KvCommit will fail if the key is not locked or is locked by another transaction.
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock == nil  {
			// key is not locked
			// TestCommitConflictRollback4B, 直接return
			return resp, nil
		} else {
			// is locked by another transaction?
			if lock.Ts != txn.StartTS {
				resp.Error = &kvrpcpb.KeyError{
					Retryable:            "error",
				}
				return resp, nil
			}
		}
		// commit write and delete lock key
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
