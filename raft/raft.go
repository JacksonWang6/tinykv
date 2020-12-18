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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

// 这个返回枚举类型的字符串的思想值得学习一下,很精巧
func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 下方是我自己添加的字段
	peers []uint64
	electionRandomTimeOut int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	//2aa 就是把Config里面的几个字段找到Raft结构体里面的对应的字段赋值即可
	r := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          &RaftLog{
			storage:         c.Storage,
			committed:       0,
			applied:         c.Applied,
			stabled:         0,
			entries:         c.Storage.(*MemoryStorage).ents,
			pendingSnapshot: &pb.Snapshot{},
		},
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             []pb.Message{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		peers: c.peers,
		electionRandomTimeOut: c.ElectionTick,
	}
	// Prs这个比较特殊, 是一个map,我们现在来初始化它
	for _, peer := range r.peers {
		// DPrintf("[%d] 正在构造Raft实例 peer = %d", r.id, peer)
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	// DPrintf("%v\n", r.RaftLog.entries)
	DPrintf("[%d] 创建Raft实例", r.id)
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              []*pb.Entry{},
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs, msg)
	// 重置计时器
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	DPrintf("[%d] 向 [%d] 发送AppendEntry包", r.id, to)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 2aa: 2aa部分这个是肯定得填的,因为选举涉及到两个部分: 一是领导人发心跳包, 二是跟随者超时成为候选人发送选举包
	// 2aa: just push it to raft.Raft.msgs and all messages the raft received will be passed to raft.Raft.Step()
	// 2aa: 由指导书可知,由于我们目前还没有实现发送和接收的逻辑,所以我们只需要将msg push进msgs里面就可以了.其他的测试代码帮我们搞好了
	// 2aa: 目前只用填一丢丢就好了, 我现在全部列出来是为了以后填写方便,此处善用IDE分屏功能(笑
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeat,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs, msg)
	DPrintf("[%d] 向 [%d] 发送心跳包", r.id, to)
}

// tick advances the internal logical clock by a single tick.
//! doc: used to advance the internal logical clock by a single tick and hence drive
//!      the election timeout or heartbeat timeout.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 2aa: 调用一次,逻辑时钟就会滴答一次,经过的时间就会+1
	// 2aa: 由于心跳包只有领导人会发送,所以只有领导人才会更新心跳包时间间隔字段
	// fix bug: tick 里面应该包含判断是否超时的逻辑等等,之前想的太过于简单了
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		// 心跳包不用随机时间
		// overtime := r.heartbeatTimeout + rand.Intn(r.heartbeatTimeout)
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// 广播心跳包
			// r.broadcastHeartBeat()
			DPrintf("[%d] 心跳超时, 准备广播心跳包", r.id)
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				Term:    r.Term,
				To: r.id,
			})
		}
	} else {
		// 由于超时时间应该是一个随机值,但是Raft结构体里面给的却是一个定值,所以我们在这个定值的基础上加一个random值即可
		r.electionElapsed += 1
		// 根据测试用例,这里应该是[et, 2*et)
		//overtime := r.electionTimeout + rand.Intn(r.electionTimeout+1)
		//fmt.Printf("%d\n", overtime)
		if r.electionElapsed >= r.electionRandomTimeOut {
			// 发起选举
			// r.launchVote()
			// fix bug: 我是真的服了: If an election timeout happened,the node should pass
			//          'MessageType_MsgHup' to its Step method and start a new election.
			// 			也就是说不能直接调用广播,必须得构造一个信息传到step里面才行,qwq
			// fmt.Printf("超时\n")
			DPrintf("[%d] 选举超时, 准备广播选举包", r.id)
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				Term:    r.Term,
				To: r.id,
			})
			r.electionRandomTimeOut = r.electionTimeout + rand.Intn(r.electionTimeout)
			// fmt.Printf("%d\n", r.electionRandomTimeOut)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 2aa: 这里采用状态机的思想来理解, 身份对应到状态机就是状态,身份的切换就是状态的改变
	DPrintf("[%d] [Term %d] 成为Follower", r.id, r.Term)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateFollower
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 2aa: 根据论文,目前只用管选举相关
	//  在转变成候选⼈后就⽴即开始选举过程
	DPrintf("[%d] [Term+1 = %d] 成为Candidate", r.id, r.Term+1)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateCandidate
	//  ⾃增当前的任期号（currentTerm）
	r.Term += 1
	//  给⾃⼰投票
	r.votes[r.id] = true
	r.Vote = r.id
	//  重置选举超时计时器
	r.electionElapsed = 0
	//  发送请求投票的 RPC 给其他所有服务器
	// r.launchVote()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	DPrintf("[%d] [Term = %d] 成为Leader", r.id, r.Term)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 成为领导人之后立即发送空的心跳包给所有的peers
	r.broadcastHeartBeat()
}

// 候选人发起选举
func (r *Raft) launchVote () {
	// var peer uint64
	if len(r.peers) == 1 {
		r.becomeLeader()
	}
	for _, peer := range r.peers {
		// fix bug: 不能跳过自己, 有一个毒瘤测试数据,就是单节点的, 如果跳过自己就收不到回信, 那么就无法执行对应的response逻辑了
		// fix bug: 后面的测试说明还是得跳过自己
		// 跳过自己
		if peer == r.id {
			// DPrintf("跳过自己")
			continue
		}
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   peer,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		DPrintf("[%d] 向 [%d] 发送了一条 选举请求", r.id, peer)
		r.msgs = append(r.msgs, msg)
	}
}

// 广播心跳包
func (r *Raft) broadcastHeartBeat() {
	// 遍历map获得key value, key就是我们想要的东西
	DPrintf("[%d] 正在广播 心跳包", r.id)
	for _, peer := range r.peers {
		// 跳过自己
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
	// 重置计时器
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
}

// 处理选举请求
func (r *Raft) handlerVote(m pb.Message)  {
	DPrintf("[%d] 正在处理来自 [%d] 的选举请求", r.id, m.From)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject: true,
	}
	// 候选人的term比我们还小,直接丑拒
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		DPrintf("[%d] 拒绝了 [%d] 的选举请求, 因为Term太小", r.id, m.From)
		return
	}
	// DPrintf("run here1: %v\n", r.RaftLog)
	// fix bug: panic: runtime error: invalid memory address or nil pointer dereference
	// 基本可以确定这个bug的原因就是r.RaftLog为nil
	// 查看测试代码,我终于知道bug的原因了, 因为创建log的逻辑需要填写在log.go里面的newlog函数里面
	logs := r.RaftLog.entries
	// DPrintf("run here2\n")

	length := uint64(len(r.RaftLog.entries))


	// fmt.Printf("%v\n", logs)
	DPrintf("LastLog: [Trem: %d] [index: %d], [%d].LastLog: [Term: %d] [index: %d]", r.id, length-1,
		m.From, m.LogTerm, m.Index)
	// 比较日志的新旧
	if length != 0 {
		if logs[length-1].Term > m.LogTerm || ((logs[length-1].Term == m.LogTerm) && m.Index < length-1) {
			// 拒绝
			r.msgs = append(r.msgs, msg)
			DPrintf("[%d] 拒绝了 [%d] 的选举请求, 因为日志太旧了", r.id, m.From)
			return
		}
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if r.Vote == None || r.Vote == m.From {
		r.Vote = m.From
		msg.Reject = false
		DPrintf("[%d] 同意了 [%d] 的选举请求", r.id, m.From)
	}
	r.msgs = append(r.msgs, msg)
}

// 处理选举请求的回信
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Reject == false {
		// 这个人向我们投票了,那么votes里面这个人置为true
		r.votes[m.From] = true
		// 然后计算总人数是否过半
		length := len(r.peers)
		sum := 0
		for _, peer := range r.peers {
			if r.votes[peer] {
				sum += 1
			}
		}
		if sum > length / 2 {
			// 成为leader
			r.becomeLeader()
		}
	}
}

// 处理心跳包的回信
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, None)
	} else {
		r.electionElapsed = 0
		r.heartbeatElapsed = 0
		r.votes = make(map[uint64]bool, 0)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 2aa: 框架代码这里的状态机的思想的应用简直太经典了
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.launchVote()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			// 领导人的term比当前的term要小
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handlerVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.launchVote()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handlerVote(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.broadcastHeartBeat()
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartBeatResponse(m)
		case pb.MessageType_MsgPropose:

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handlerVote(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("[%d] 正在处理来自 [%d] 的 AppendEntry 包", r.id, m.From)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject: true,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		DPrintf("[%d] 拒绝了 [%d] 的 AppendEntry 包, 因为Term太小了", r.id, m.From)
		return
	}
	// 对于领导人是否做小弟, 还需要看日志索引值的比较结果,但2aa暂未实现log
	// 否则收到了心跳包,老老实实的当小弟
	r.becomeFollower(m.Term, m.From)
	DPrintf("[%d] 接收了来自 [%d] 的 AppendEntry 包, 并且回了信", r.id, m.From)
	// 回信
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("[%d] 正在处理来自 [%d] 的心跳包", r.id, m.From)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject: true,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		DPrintf("[%d] 拒绝了 [%d] 的心跳包, 因为Term太小了", r.id, m.From)
		return
	}
	// 否则收到了心跳包,老老实实的当小弟
	r.becomeFollower(m.Term, m.From)
	DPrintf("[%d] 接收了来自 [%d] 的心跳包, 并且回了信", r.id, m.From)
	// 回信
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
