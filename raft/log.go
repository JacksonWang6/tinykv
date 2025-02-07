// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 被持久化进storage的最后一个log的index
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

func (l *RaftLog) firstLogIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	fi, _ := l.storage.FirstIndex()
	return fi
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 根据给定的storage恢复log
	raftLog := RaftLog{
		storage:         storage,
		//  in here you also need to interact with the upper application by the Storage interface defined
		//  in raft/storage.go to get the persisted data like log entries and snapshot.
		// pendingSnapshot: &storage.(*MemoryStorage).snapshot,
		// 这两个变量赋啥值啊qwq
		committed: None,
		applied: None,
	}
	// Term returns the term of entry i, which must be in the range [FirstIndex()-1, LastIndex()].
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	// log entries with index <= stabled are persisted to storage.
	//! NOTE: i'm not sure

	// stabled是没有被持久化的第一条日志的下标, 而last是已经被持久化的最后一条下标, 所以stabled=last+1, 注意commit了不代表被持久化了
	// ps: 上面的我也不知道说得对不对, 不过目前来看没有大的问题
	// update: 上面的说法问题很大
	raftLog.stabled = last
	raftLog.applied = first-1
	// 我终于知道这里为什么会panic了, 因为InitialState里面直接解引用了snapshot, 但是却并没有对nil进行判断
	// first+1是因为下标为0的位置是一个空的无意义的log, last加1是因为切片操作右边是开区间
	if first > last {
		raftLog.entries = []pb.Entry{}
	} else {
		raftLog.entries, _ = storage.Entries(first, last+1)	// 返回的是storage当中index为[first, last]的区间
	}
	DPrintf("调试newLog: first=%d, last=%d, stabled=%d, entries=%v", first, last, raftLog.stabled, raftLog.entries)
	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// fix bug, 这里的stabled是index,不是下标哇
	// 这里在2C出大问题了啊
	ents := make([]pb.Entry, 0)
	for i := l.stabled+1; i <= l.LastIndex(); i++{
		var entry pb.Entry
		if len(l.entries) == 0 || l.entries[0].Index > i || i == 0 {
			entry = pb.Entry{Term: 0, Index: l.LastIndex()}
		} else {
			entry = l.entries[i - l.entries[0].Index]
		}
		ents = append(ents, entry)
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// 我发现2C里面出现crash之后, 我之前log里写的函数好多都有问题啊
	ent := make([]pb.Entry, 0)
	for i := l.applied+1; i <= l.committed; i++ {
		var entry pb.Entry
		if len(l.entries) == 0 || l.entries[0].Index > i || i == 0 {
			entry = pb.Entry{Term: 0, Index: l.LastIndex()}
		} else {
			entry = l.entries[i - l.entries[0].Index]
		}
		ent = append(ent, entry)
	}
	return ent
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 终于有一点懂了, logEntry的结构如文件最前面的注释所示,里面有一部分是已经被持久化到storage里面的,还有一部分没有被持久化
	// 我们需要根据是否存在没有被持久化的来判断这个index怎么获得
	var res uint64
	// 出大问题了, 2C的snapshot的测试挂掉了,挂掉的原因是我们storage是空的,只是从快照读取了东西, 所以这里需要特判一下快照是否存在
	// 但是这个问题只有接收了快照的第一个tick存在,因为下一个tick在ready函数里面该变量就会置为nil,并且hasReady会为true,最后applysnapshot
	// 将状态应用到底层
	if !IsEmptySnap(l.pendingSnapshot) {
		res = l.pendingSnapshot.Metadata.Index
	} else {
		if len(l.entries) > 0 {
			res = l.entries[len(l.entries)-1].Index
		} else {
			res, _ = l.storage.LastIndex()
		}
	}
	return res
}

// 返回最后一条日志的term
func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 该函数功能十分强大,能够返回整个log entry里面任何一个合法的index的term号
	// 我想我之前可能一直理解错了, 其实raftLog里面的entries是把所有的日志都存储起来了, 只不过其中有一部分还被持久化到storage中了而已
	// first, _ := l.storage.FirstIndex()
	// first -= 1
	// fix bug: 这里出现了一个非常诡异的bug, 就是在raft里面调用term,err了,然后panic了,原因就是在这个函数中执行的是最下面的
	// 那个storage的term函数,这个函数在peerstorage里面的实现中有一个check函数,他会与ps.truncatedIndex()相比较,而这里比它小了,
	// 就err了
	if len(l.entries) > 0 && i >= l.entries[0].Index {
		fi := l.entries[0].Index
		return l.entries[i-fi].Term, nil
	}
	// fix bug: 引入了snapshot之后,如同LatIndex一样,这个也需要做出修改
	term, err := l.storage.Term(i)
	// 如果不在storage里面会返回一个ErrUnavailable的错误
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		// 在快照里面我们只知道最后一个日志的index和term
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}
