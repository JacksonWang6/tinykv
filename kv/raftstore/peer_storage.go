package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// gennerate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	// fmt.Printf("startkey: %v, endkey: %v\n", startKey, endKey)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	// 在这里进行错误检查
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		// fmt.Printf("run here: %d\n", ps.raftState.LastIndex)
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				log.Infof("%v 成功生成snapshot", ps.Tag)
				snapshot = *s
			}
		default:
			log.Infof("%v snapshot正在生成中", ps.Tag)
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		// 如果最小的比已经截断的最大的index还要小,那么就返回ErrCompacted
		log.Infof("ErrCompacted, low: %v, ps.truncatedIndex(): %v", low, ps.truncatedIndex())
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	// 我们还需要在最开始特判entries是否为空, 不然之后的获取最后一个日志条目的操作会出错
	// fmt.Printf("%v\n", entries)
	if len(entries) == 0 {
		// 什么也不做
		return nil
	}
	// 根据测试, 这里我们需要做的工作就是把entries里面的东西给他append进去
	// 根据指导书, 这里应该用setmeta, 但是具体用法应该不像测试用例的那样,因为我们这里明显是log条目
	// 于是全局搜索发现runner_test.go里面有一个用法的示例
	// raftWb.SetMeta(meta.RaftLogKey(regionId, i), &eraftpb.Entry{Data: []byte("entry")})
	// fix bug: 这里的index不是数组下标,就是index
	lastIdx := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	regionId := ps.region.GetId()
	for _, entry := range entries {
		err := raftWB.SetMeta(meta.RaftLogKey(regionId, entry.Index), &entry)
		if err != nil {
			log.Infof("SetMeta error")
			return err
		}
	}

	// TODO: delete any previously appended log entries which will never be committed.
	prevIdx, _ := ps.LastIndex()
	for i := lastIdx+1; i <= prevIdx; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionId, i))
	}
	// fix bug: 最后需要更新lastindex以及lastTerm, 初始时lastindex为5,导致第一个测试的index为5的那个点过不了
	// 然后又发现如果需要获得最后一条日志的index与任期号,那么我们需要对这次传进来的entries进行判断
	ps.raftState.LastIndex = lastIdx
	ps.raftState.LastTerm = lastTerm
	return nil
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	var applySnapResult ApplySnapResult
	if snapData.Region.RegionEpoch.ConfVer >= ps.region.RegionEpoch.ConfVer &&
		snapData.Region.RegionEpoch.Version >= ps.region.RegionEpoch.Version {
		ps.snapState.StateType = snap.SnapState_Applying
		// call ps.clearMeta and ps.clearExtraData to delete stale data
		if ps.isInitialized() {
			err := ps.clearMeta(kvWB, raftWB)
			if err != nil {
				panic(err)
			}
			kvWB.WriteToDB(ps.Engines.Kv)
			raftWB.WriteToDB(ps.Engines.Raft)
			kvWB.Reset()
			raftWB.Reset()
			ps.clearExtraData(snapData.Region)
		}

		// RaftLocalState
		ps.raftState.LastIndex = snapshot.Metadata.Index
		ps.raftState.LastTerm = snapshot.Metadata.Term
		log.Infof("snapshot.Metadata.Index: %v", snapshot.Metadata.Index)
		// RaftApplyState
		ps.applyState.TruncatedState.Index = snapshot.Metadata.Index
		ps.applyState.TruncatedState.Term = snapshot.Metadata.Term
		log.Infof("更新了 TruncatedState.Index: %d", ps.applyState.TruncatedState.Index)
		ps.applyState.AppliedIndex = snapshot.Metadata.Index


		// fix bug: 要先更新状态之后再来执行RegionTask
		// send runner.RegionTaskApply task to region worker through PeerStorage.regionSched
		// and wait until region worker finish.
		notify := make(chan bool, 1)
		// fix bug: 这里掉了一个&, 导致了stop集群时卡死了
		ps.regionSched <- &runner.RegionTaskApply{
			RegionId: snapData.Region.Id,
			Notifier: notify,
			SnapMeta: snapshot.Metadata,
			StartKey: snapData.Region.GetStartKey(),
			EndKey:   snapData.Region.GetEndKey(),
		}
		// wait until region worker finish
		<-notify
		log.Infof("regionSched finished")

		applySnapResult = ApplySnapResult{
			PrevRegion: ps.region,
			Region:     snapData.Region,
		}

		// don’t forget to persist these states to kvdb and raftdb, raftdb will setmeta at outer function
		kvWB.SetMeta(meta.ApplyStateKey(snapData.Region.GetId()), ps.applyState)
		// update region
		ps.SetRegion(snapData.Region)
		meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)
	}
	return &applySnapResult, nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
// ALREADY PASS ALL peer_storage tests!!! ^_^
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	// what this function does is to save the data in raft.Ready to badger,
	// including append log entries and save the Raft hard state.
	// To append log entries, simply save all log entries at raft.Ready.Entries to raftdb and
	// delete any previously appended log entries which will never be committed.
	// Also update the peer storage’s RaftLocalState and save it to raftdb.
	if ready == nil {
		log.Infof("%v ready == nil", ps.Tag)
		return nil, nil
	}
	raftWB := new(engine_util.WriteBatch)
	kvWB := new(engine_util.WriteBatch)
	var applySnapResult *ApplySnapResult
	var err error
	// log.Infof("为什么没有运行到这里?: %v", ready.Snapshot)
	// 若检测到有Snapshot则调用ApplySnapshot更新相关信息进行处理
	// 需要先执行这个, 因为snapshot里面regionid可能更新了
	if !raft.IsEmptySnap(&ready.Snapshot) {
		log.Infof("为什么没有运行到这里?")
		applySnapResult, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return applySnapResult, err
		}
		kvWB.WriteToDB(ps.Engines.Kv)
	}
	// append log entries
	err = ps.Append(ready.Entries, raftWB)
	if err != nil {
		log.Infof("Append error")
		return nil, err
	}

	// To save the hard state is also very easy, just update peer storage’s RaftLocalState.HardState
	// and save it to raftdb.
	if !raft.IsEmptyHardState(ready.HardState) {
		// 不为空说明发生了变化
		// update peer storage’s RaftLocalState.HardState
		ps.raftState.HardState = &ready.HardState
	}
	// save it to raftdb. 利用setMata
	raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)

	// 一共有两个db, 一个是raftdb, 一个是kvdb, writeBatch存进哪一个呢
	// save it to raftdb, because raftdb stores raft log and RaftLocalState
	// and RaftLocalState Used to store HardState of the current Raft and the last Log Index.
	raftWB.WriteToDB(ps.Engines.Raft)
//	log.Infof("raftstate.index: %v", ps.raftState.LastIndex)
	// 这里目前不太清楚返回的applySnapResult怎样赋值
	return applySnapResult, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
