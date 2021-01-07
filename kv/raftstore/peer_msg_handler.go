package raftstore

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// So HandleRaftReady should get the ready from Raft module and do corresponding actions
	// like persisting log entries, applying committed entries and sending raft messages to other peers
	// through the network.
	// 当一个 RawNode ready 之后，我们需要对 ready 里面的数据做一系列处理，包括将 entries 写入 Storage，
	// 发送 messages，apply committed_entries 以及 advance 等。
	// 这些全都在 Peer 的 handle_raft_ready 系列函数里面完成。(来自官方博客)
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		// saveToStorage(rd.State, rd.Entries, rd.Snapshot)
		applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		if applySnapResult != nil {
			// fix panic: find no region
			d.peerStorage.SetRegion(applySnapResult.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			d.ctx.storeMeta.setRegion(applySnapResult.Region, d.peer)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			d.ctx.storeMeta.Unlock()
		}
		// send(rd.Messages)
		// Transport 对象，用来让 Peer 发送 message, 这个msgs就是Raft信箱中的msgs, 最开始在Ready函数里面还忘记了给这个赋值
		// log.Infof("[%v] 正在发送msgs: %v", d.Tag, rd.Messages)
		d.Send(d.ctx.trans, rd.Messages)
		// process(entry), 就是将已经提交的条目应用到状态机当中
		// 这一步的处理过程非常复杂
		// 官方博客讲的太精辟啦, 我把一些重要信息粘贴过来啦, 解决了我的不少疑惑
		// 对于 committed_entries 的处理，Peer 会解析实际的 command，调用对应的处理流程，执行对应的函数
		// 为了保证数据的一致性，Peer 在 execute 的时候，都只会将修改的数据保存到 RocksDB 的 WriteBatch 里面，
		// 然后在最后原子的写入到 RocksDB，写入成功之后，才修改对应的内存元信息。如果写入失败，我们会直接 panic，保证数据的完整性。
		if len(rd.CommittedEntries) > 0 {
			// 判断是否存在已经提交了的日志
			kvWB := new(engine_util.WriteBatch)
			for _, entry := range rd.CommittedEntries {
				d.process(&entry, kvWB)
				// 这里的判断必不可少, 不然会导致destory之后又写入了applystate的信息, 导致newpeer时比较raftlogindex
				// 与applyindex时出现大问题
				if d.stopped {
					return
				}
			}
			// Do not forget to update and persist the apply state when applying the log entries.
			d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			var err error
			err = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic(err)
			}
			log.Infof("[%v] term: %v 写入kvDB成功", d.Tag, d.Term())
		}
		// s.Node.Advance(rd)
		// 更新状态机的状态
		d.RaftGroup.Advance(rd)
	}
}

// 我想我懂了: 客户端提交请求的时候是只能提交给那一届任期的leader的, 然后那个时候的leader的proposals里面会添加对应的回调函数cb
// 当这一条日志被提交然后应用的时候每台机器就会在proposals里面找看看有没有回调函数,只有这条日志提交的时候为leader的机器才会有cb,
// 但是这台机器可能挂掉了又重连了换届了之类的, 所以index+term唯一标示了它, 缺一不可
func (d *peerMsgHandler) getProposal (index, term uint64) proposal {
	if len(d.proposals) == 0 {
		log.Infof("[%v] proposals为空", d.Tag)
		return proposal{
			cb: nil,
		}
	}
	p := d.proposals[0]
	if term < p.term {
		return proposal{
			cb: nil,
		}
	}
	d.proposals = d.proposals[1:]
	if p.index == index {
		return *p
	}
	// 否则没有找到
	log.Infof("没有找对对应下标的proposal")
	return proposal{
		cb: nil,
	}
}

// 处理那四种普通请求的函数
func (d *peerMsgHandler) processRequest (entry *pb.Entry, kvWB *engine_util.WriteBatch, cmd *raft_cmdpb.RaftCmdRequest,) {
	p := d.getProposal(entry.Index, entry.Term)
	// 如果在 Applied Index + 1 到 Last Index 之间的 Proposal 有修改 Region Epoch 的操作，
	// 新 Proposal 就有可能会在应用的时候被跳过。
	// 所以我们在process之前还需要再检查一遍
	for _, request := range cmd.Requests {
		key := d.getKey(request)
		if key != nil {
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				if p.cb != nil {
					if p.term == entry.Term {
						p.cb.Done(ErrResp(err))
					} else {
						NotifyStaleReq(p.term, p.cb)
					}
				}
				return
			}
		}
	}
	// 非 Admin Request， Proposal 中的 version 与当前的不相等。
	if cmd.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
		if p.cb != nil {
			if p.term == entry.Term {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
			} else {
				NotifyStaleReq(p.term, p.cb)
			}
		}
		return
	}
	if len(cmd.Requests) == 0 {
		return
	}
	responses := raft_cmdpb.RaftCmdResponse{
		Header:               &raft_cmdpb.RaftResponseHeader{
			CurrentTerm:          entry.Term,
		},
	}
//	txn := false
	for _, command := range cmd.Requests {
		log.Infof("[%v] 解析出来的命令: %v", d.Tag, command)
		// 如果是修改的操作,那么我们需要将修改后的状态应用到状态机里面
		// You can regard kvdb as the state machine mentioned in Raft paper
		if command.CmdType == raft_cmdpb.CmdType_Put || command.CmdType == raft_cmdpb.CmdType_Delete {
			switch command.CmdType {
			case raft_cmdpb.CmdType_Put:
				log.Infof("[%v] 正在执行SetCF: cf: %v key: %v, value: %v", d.Tag, command.Put.Cf, string(command.Put.Key), string(command.Put.Value))
				kvWB.SetCF(command.Put.Cf, command.Put.Key, command.Put.Value)
			case raft_cmdpb.CmdType_Delete:
				log.Infof("[%v] 正在执行DeleteCF: key: %v", d.Tag, string(command.Delete.Key))
				kvWB.DeleteCF(command.Delete.Cf, command.Delete.Key)
			}
			// 发现了一个神奇的bug, scan的时候want与got之间有时候就差最后一个key的值,但是我看日志里面明明写入了啊
			// 所以我干脆每次set之后都写入一次
			kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			// 清空kvWB
			kvWB.Reset()
		}
		var response *raft_cmdpb.Response
		switch command.CmdType {
		case raft_cmdpb.CmdType_Put:
			// 前面已经应用到状态机里面了,所以这里只用构造一个response就可以了
			response = &raft_cmdpb.Response{
						CmdType:              raft_cmdpb.CmdType_Put,
						Put:                  &raft_cmdpb.PutResponse{},
					}
			responses.Responses = append(responses.Responses, response)
		case raft_cmdpb.CmdType_Get:
			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, command.Get.Cf, command.Get.Key)
			if err != nil {
				log.Infof("[%v] Get操作失败,kvDB当中不存在 cf: %v key: %v",
					d.Tag, string(command.Get.Cf), string(command.Get.Key))
				p.cb.Done(ErrResp(err))
				return
			}
			log.Infof("[%v] Get操作成功, key: %v, value: %v", d.Tag, string(command.Get.Key), string(value))
			response = &raft_cmdpb.Response{
				CmdType:              raft_cmdpb.CmdType_Get,
				Get:                  &raft_cmdpb.GetResponse{
					Value:                value,
				},
			}
			responses.Responses = append(responses.Responses, response)
		case  raft_cmdpb.CmdType_Delete:
			// 前面已经应用到状态机里面了,所以这里只用构造一个response就可以了
			response = &raft_cmdpb.Response{
				CmdType:              raft_cmdpb.CmdType_Delete,
				Delete:               &raft_cmdpb.DeleteResponse{},
			}
			responses.Responses = append(responses.Responses, response)
		case raft_cmdpb.CmdType_Snap:
			// !!!!!! fix bug
			// 这里必须得传一个深拷贝过去:
			// 不然的话如果直接传一个指针过去那么当在scan的时候发生split region将会发生错误
			// 因为跨region的scan它的逻辑是看当前region是否遍历完，遍历完之后更新key为endkey，
			// 然后获取下一个连续的新region，但是这个时候发生了split就会导致当前region的endkey发生变化，
			// 就会导致key更新出问题，这时key就会更新为splitKey的值, 然后就回到前面split的位置又scan了一遍
			// 这个bug找了我整整一天啊!!!
			var region metapb.Region
			region.StartKey = d.Region().StartKey
			region.EndKey = d.Region().EndKey
			region.RegionEpoch = &metapb.RegionEpoch{
				ConfVer:              d.Region().RegionEpoch.ConfVer,
				Version:              d.Region().RegionEpoch.Version,
			}
			region.Id = d.Region().Id
			for _, peer := range d.Region().Peers {
				region.Peers = append(region.Peers, &metapb.Peer{
					Id:                   peer.Id,
					StoreId:              peer.StoreId,
				})
			}
			response = &raft_cmdpb.Response{
				CmdType:              raft_cmdpb.CmdType_Snap,
				Snap:              	  &raft_cmdpb.SnapResponse{
					Region:               &region,
				},
			}
			responses.Responses = append(responses.Responses, response)
			//txn = true
		}
	}
	// Record the callback of the command when proposing, and return the callback after applying.
	// 处理完毕之后,还需要看看是否执行回调函数
	// returns the response by callback
	if p.cb == nil {
		return
	}
	if p.term != entry.Term {
		log.Infof("[%v] ErrRespStaleCommand", d.Tag)
		NotifyStaleReq(p.term, p.cb)
		return
	}
	//if txn {
		p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	//}
	log.Infof("[%v] 正在执行回调,返回responses", d.Tag)
	p.cb.Done(&responses)
}

func (d *peerMsgHandler) processAdminRequest(entry *pb.Entry, kvWB *engine_util.WriteBatch, cmd *raft_cmdpb.RaftCmdRequest) {
	p := d.getProposal(entry.Index, entry.Term)
	// check admin request
	err := util.CheckRegionEpoch(cmd, d.Region(), true)
	if err != nil {
		log.Infof("CheckRegionEpoch err: %v", err)
		if p.cb != nil {
			if p.term == entry.Term {
				p.cb.Done(ErrResp(err))
			} else {
				NotifyStaleReq(p.term, p.cb)
			}
		}
	}
	response := raft_cmdpb.RaftCmdResponse{
		Header:               &raft_cmdpb.RaftResponseHeader{
			CurrentTerm:          entry.Term,
		},
	}
	adminCommand := cmd.AdminRequest
	switch adminCommand.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		log.Infof("[%v] 正在 process AdminCmdType_CompactLog命令: %v", d.Tag, adminCommand.CompactLog)
		// CompactLogRequest modifies metadata, namely updates the RaftTruncatedState which is
		// in the RaftApplyState
		compactLog, applyState := adminCommand.GetCompactLog(), d.peerStorage.applyState
		if compactLog.CompactIndex >= applyState.TruncatedState.Index {
			// HandleRaftReady中在apply命令时若发现该命令是CompactLogRequest，则修改TruncatedState
			applyState.TruncatedState.Index = compactLog.CompactIndex
			applyState.TruncatedState.Term = compactLog.CompactTerm
			// 目前只是先写到WB里面, 之后统一写回DB
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
			// After that, you should schedule a task to raftlog-gc worker by ScheduleCompactLog.
			// 发送一个raftLogGcTask到raftLogGCTaskSender，在另一个线程中对raftdb中的日志进行压缩
			// firstIndex字段压根没使用,随便传一个东西好了
			d.ScheduleCompactLog(0, applyState.TruncatedState.Index)
			response.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
				CompactLog: &raft_cmdpb.CompactLogResponse{},
			}
		}
	case raft_cmdpb.AdminCmdType_ChangePeer:
		log.Infof("[%v] 正在 process AdminCmdType_ChangePeer: %v", d.Tag, adminCommand.ChangePeer)
		// After the log is committed, change the RegionLocalState, including RegionEpoch and Peers in Region
		region := d.Region()
		// RegionEpoch’s conf_ver increases during ConfChange
		changePeer := cmd.AdminRequest.ChangePeer.Peer
		var msg pb.ConfChange
		msg.Unmarshal(entry.Data)
		switch msg.ChangeType {
		case pb.ConfChangeType_AddNode:
			if cmd.Header.RegionEpoch.ConfVer >= d.peerStorage.region.RegionEpoch.ConfVer {
				found := false
				for _, p := range region.Peers {
					if p.Id == changePeer.Id && p.StoreId == changePeer.StoreId {
						found = true
						break
					}
				}
				if !found {
					// fix bug: 一定要先更新, 再WriteRegionState, 不然checksnap的时候会报confversion的错
					// 如果不存在,那么添加进去
					region.Peers = append(region.Peers, changePeer)
					region.RegionEpoch.ConfVer = cmd.Header.RegionEpoch.ConfVer+1
					log.Infof("[%v] RegionEpoch.ConfVer: %v", d.Tag, region.RegionEpoch.ConfVer)
					// Do not forget to update the region state in storeMeta of GlobalContext
					d.ctx.storeMeta.Lock()
					d.ctx.storeMeta.setRegion(region, d.peer)
					d.ctx.storeMeta.Unlock()
					meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
					d.insertPeerCache(changePeer)
				}
			}
		case pb.ConfChangeType_RemoveNode:
			if cmd.Header.RegionEpoch.ConfVer >= d.peerStorage.region.RegionEpoch.ConfVer {
				for index, peer := range region.Peers {
					if peer.Id == msg.NodeId && peer.StoreId == changePeer.StoreId {
						region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
						region.RegionEpoch.ConfVer = cmd.Header.RegionEpoch.ConfVer+1
						log.Infof("[%v] RegionEpoch.ConfVer: %v", d.Tag, region.RegionEpoch.ConfVer)
						// Do not forget to update the region state in storeMeta of GlobalContext
						d.ctx.storeMeta.Lock()
						d.ctx.storeMeta.setRegion(region, d.peer)
						d.ctx.storeMeta.Unlock()
						meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
						d.removePeerCache(msg.NodeId)
						break
					}
				}
				if d.Meta.Id == changePeer.Id {
					// 停机, 立刻返回
					d.destroyPeer()
					return
				}
			}
		}
		kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		kvWB.Reset()
		// Call ApplyConfChange() of raft.RawNode
		d.RaftGroup.ApplyConfChange(msg)
		response.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:              raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer:           &raft_cmdpb.ChangePeerResponse{
				Region:               d.Region(),
			},
		}
	case raft_cmdpb.AdminCmdType_Split:
		splitCmd := adminCommand.Split
		log.Infof("[%v] 正在 process AdminCmdType_Split: %v", d.Tag, splitCmd)
		// 检查
		// 首先检测split key是否在start key 和end key之间，不满足返回
		err := util.CheckKeyInRegion(splitCmd.SplitKey, d.Region())
		if err != nil {
			log.Infof("splitkey not in region")
			// 不在
			if p.cb == nil {
				return
			}
			if p.term != entry.Term {
				log.Infof("[%v] ErrRespStaleCommand", d.Tag)
				NotifyStaleReq(p.term, p.cb)
				return
			}
			p.cb.Done(ErrResp(err))
			return
		}
		log.Infof("run here")
		var regions []*metapb.Region
		if cmd.Header.RegionEpoch.Version >= d.peerStorage.region.RegionEpoch.Version {
			// 创建新region 原region和新region key范围为start~split，split~end
			// The corresponding Peer of this newly-created Region should be created by createPeer() and
			// registered to the router.regions. And the region’s info should be inserted into
			// regionRanges in ctx.StoreMeta.
			log.Infof("run here")
			region := d.Region()
			newRegion := new(metapb.Region)
			newRegion.Id = splitCmd.NewRegionId
			region.RegionEpoch.Version++
			newRegion.RegionEpoch = &metapb.RegionEpoch{
				ConfVer:              region.RegionEpoch.ConfVer,
				Version:              region.RegionEpoch.Version,
			}
			for i, peer := range region.Peers {
				if i >= len(splitCmd.NewPeerIds) {
					break
				}
				newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
					Id:                   splitCmd.NewPeerIds[i],
					StoreId:              peer.GetStoreId(),
				})
			}
			newRegion.StartKey = splitCmd.SplitKey
			newRegion.EndKey = region.EndKey
			region.EndKey = splitCmd.SplitKey
			log.Infof("original: [%v, %v) new: [%v, %v)", hex.EncodeToString(region.StartKey),
				hex.EncodeToString(region.EndKey), hex.EncodeToString(newRegion.StartKey), hex.EncodeToString(newRegion.EndKey))
			newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
			if err != nil {
				panic(err)
			}
			d.ctx.router.register(newPeer)
			for _, peer := range newRegion.Peers {
				newPeer.insertPeerCache(peer)
			}
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.setRegion(region, d.peer)
			d.ctx.storeMeta.setRegion(newRegion, newPeer)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
			d.ctx.storeMeta.Unlock()
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
			// 下面的这几步我也不知道为什么要这样做, 16级学长给的一份指导里面是这样说的qwq
			// 设置SizeDiffHint为0, 设置ApproximateSize为0
			// 如果是leader,则调用HeartbeatScheduler
			// 对于新创建的region,调用createPeer,并调用insertPeerCache插入peer信息, 发送MsgTypeStart信息
			d.SizeDiffHint = 0
			d.ApproximateSize = nil
			if d.IsLeader() {
				// 如果是leader,则调用HeartbeatScheduler
				d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			}
			newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
			d.ctx.router.send(newRegion.Id, message.Msg{
				Type:     message.MsgTypeStart,
				RegionID: newRegion.GetId(),
			})
			regions = append(regions, region)
			regions = append(regions, newRegion)
		}
		response.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:              raft_cmdpb.AdminCmdType_Split,
			Split:           	  &raft_cmdpb.SplitResponse{
				Regions:              regions,
			},
		}
	}
	// 处理回应
	if p.cb == nil {
		return
	}
	if p.term != entry.Term {
		log.Infof("[%v] ErrRespStaleCommand", d.Tag)
		NotifyStaleReq(p.term, p.cb)
		return
	}
	log.Infof("[%v] 正在执行回调,返回response, command type: %v", d.Tag, adminCommand.CmdType)
	p.cb.Done(&response)
}

// apply已经提交的日志
// 重构了代码
func (d *peerMsgHandler) process(entry *pb.Entry, kvWB *engine_util.WriteBatch) error {
	if entry.Data == nil {
		log.Infof("entry.Data is nil")
		return nil
	}
	log.Infof("[%v] 正在process Entry: %v", d.Tag, entry)
	message := &raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == pb.EntryType_EntryConfChange {
		var confChange pb.ConfChange
		err := confChange.Unmarshal(entry.Data)
		if err != nil {
			log.Infof("Unmarshal confChange error")
			return err
		}
		err = message.Unmarshal(confChange.Context)
		if err != nil {
			log.Infof("Unmarshal confChange.Context error")
			return err
		}
	} else {
		err := message.Unmarshal(entry.Data)
		if err != nil {
			log.Infof("process Unmarshal error: %v", err)
			return err
		}
	}
	log.Infof("message: %v", message)
	if message.AdminRequest != nil {
		log.Infof("process admin")
		d.processAdminRequest(entry, kvWB, message)
		return nil
	}

	if len(message.Requests) > 0 {
		d.processRequest(entry, kvWB, message)
		return nil
	}
	// should not run here
	panic(0)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		// ErrNotLeader: the raft command is proposed on a follower. so use it to let client try other peers.
		// 框架代码已经在这里帮我们实现好了,所以我们只用调用该函数,然后判断err是否为nil并返回err即可\
//		log.Infof("[%v] ErrNotLeader, leaderId: %v", d.Tag, leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
//		log.Infof("[%v] err preProposeRaftCommand", d.Tag)
//		log.Infof("这个错误的提交为%v", msg)
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 将代码重构了, 划分成函数, 方便维护
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeRequest(msg, cb)
	}
}

func (d *peerMsgHandler) addProposal(cb *message.Callback) {
	// 添加新的proposal
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	admin := msg.AdminRequest
	log.Infof("[%v] 正在 propose AdminRequest: %v", d.Tag, admin)
	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// bug here: 我终于知道为什么一直没有处理那个compact请求了, 因为这里propose请求的时候出现了错误
		// request, err := admin.Marshal()
		request, err := msg.Marshal()
		log.Infof("[%v] 正在 propose compact: %v", d.Tag, admin)
		if err != nil {
			panic(err)
		}
		log.Infof("正在添加新的proposal, index: %v, term: %v", d.nextProposalIndex(), d.Term())
		d.addProposal(cb)
		d.RaftGroup.Propose(request)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		//  just need to call the TransferLeader() method of RawNode instead of Propose() for
		//  TransferLeader command
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		// 直接返回response
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header:               &raft_cmdpb.RaftResponseHeader{
				CurrentTerm:          d.Term(),
			},
			AdminResponse:        &raft_cmdpb.AdminResponse{
				CmdType:              raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader:       &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// Test code schedules the command of one conf change multiple times until the conf change
		// is applied, so you need to consider how to ignore the duplicate commands of same conf change.
		// 如果PendingConfIndex的值大于 d.peerStorage.AppliedIndex(),那么说明存在还未被应用的confchang指令。直接返回
		pendingConfIndex := d.RaftGroup.Raft.PendingConfIndex
		if pendingConfIndex > d.peerStorage.AppliedIndex() {
			// 当前还有conf change并且这个conf change还没有被应用, 那么忽略后面的
			log.Infof("当前conf change还没有被应用")
			cb.Done(ErrResp(errors.New("error")))
			return
		}
		changePeer := msg.AdminRequest.ChangePeer
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		request := pb.ConfChange{
			ChangeType:           changePeer.ChangeType,
			NodeId:               changePeer.Peer.Id,
			Context: 			  context,
		}
		log.Infof("正在添加新的proposal, index: %v, term: %v", d.nextProposalIndex(), d.Term())
		d.addProposal(cb)
		// Propose conf change admin command by ProposeConfChange
		d.RaftGroup.ProposeConfChange(request)
	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(admin.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		request, err := msg.Marshal()
		log.Infof("[%v] 正在 propose Split: %v", d.Tag, admin)
		if err != nil {
			panic(err)
		}
		log.Infof("正在添加新的proposal, index: %v, term: %v", d.nextProposalIndex(), d.Term())
		d.addProposal(cb)
		d.RaftGroup.Propose(request)
	}
}

func (d *peerMsgHandler) getKey (request *raft_cmdpb.Request) []byte {
	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		return request.Get.Key
	case raft_cmdpb.CmdType_Delete:
		return request.Delete.Key
	case raft_cmdpb.CmdType_Put:
		return request.Put.Key
	default:
		return nil
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 在这里需要判断key是否在region里面
	for _, request := range msg.Requests {
		key := d.getKey(request)
		if key != nil {
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}
	}

	if len(msg.Requests) == 0 {
		return
	}

	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	log.Infof("正在添加新的proposal, index: %v, term: %v, cmd: %v", d.nextProposalIndex(), d.Term(), msg)
	d.addProposal(cb)
	// 向RaftGroup提交command
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// ??? 这个firstIndex字段根本就没有使用
func (d *peerMsgHandler) ScheduleCompactLog(firstIndex uint64, truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Infof("isInitialized: %v", isInitialized)
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	// bug: 这里会频繁触发compact, 原因是trunctedidx没有更新,一直是5
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		log.Infof("applied index: %v, firstidx: %v", appliedIdx, firstIdx)
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	// 然后在这里提交compact请求, 讲道理会更新truncateidx啊, 真是奇怪哇
	log.Infof("提交了 compact请求")
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
