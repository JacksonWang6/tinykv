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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
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
	reject int
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
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		// 这个bug藏得太深了,空数组应该是nil,不应该是[],我也不知道为什么
		msgs:             nil,
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
	// fix bug: panic find no region for 3020....
	hardState, confState, _ := r.RaftLog.storage.InitialState()
	if c.peers == nil {
		r.peers = confState.GetNodes()
	}
	// fmt.Printf("peers: %v", c.peers)
	lastindex := r.RaftLog.LastIndex()
	// Prs这个比较特殊, 是一个map,我们现在来初始化它
	for _, peer := range r.peers {
		// DPrintf("[%d] 正在构造Raft实例 peer = %d", r.id, peer)
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Match: lastindex,
				// 因为storege里面的源码显示log的下标从1开始
				Next:  lastindex+1,
			}
			continue
		}
		r.Prs[peer] = &Progress{
			Match: 0,
			// 因为storege里面的源码显示log的下标从1开始
			Next:  lastindex+1,
		}
	}

	// 根据测试用例, 我们还要从hardState中获取vote和term: TestLeaderElectionOverwriteNewerLogs2AB
	// hardState, _, _ := r.RaftLog.storage.InitialState()
	// update: 改用Get函数,而不是直接获得对应的属性,因为函数增加了错误处理
	r.Vote = hardState.GetVote()
	r.Term = hardState.GetTerm()
	r.RaftLog.committed = hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	// DPrintf("%v\n", r.RaftLog.entries)
	DPrintf("[%d] [Term: %d] 创建Raft实例", r.id, r.Term)
	return &r
}

func (r *Raft) sendSnapShot(to uint64) {
//	log.Infof("[%d] 正在向 %d 发送snapshot", r.id, to)
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			// snapshot正在生成,暂时不可用
//			log.Infof("[%d] snapshot ErrSnapshotTemporarilyUnavailable", r.id)
			return
		}
		log.Infof("[%d] snapshot err", r.id)
		// 真的是奇怪,ErrSnapshotTemporarilyUnavailable这个错误居然会运行到这里然后panic? 不应该在上面那个if那里return吗
		// fix bug: it is not raft.ErrSnapshotTemporarilyUnavailable, it is just ErrSnapshotTemporarilyUnavailable
//		panic(err)
		// 这里还可能报错连续请求snap次数太多, 暂时直接返回
		return
	}
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgSnapshot,
		To:                   to,
		From:                 r.id,
		Snapshot:             &snapshot,
		Term: 				  r.Term,
	}
//	log.Infof("[%d] 成功向 %d 发出snapshot", r.id, to)
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id{
		return false
	}
	// log下标从1开始
	// 本来2aa过了, 结果写2ab写了一半之后测了一下2aa又挂了, 心态崩了, 调试了一下之后发现, 挂掉的原因就是心跳包与日志包的log的index等
	// 不对, 导致比较日志新旧的时候出现了问题, 还得好好研究一下日志这一块, 有点难顶
	// update: 现在终于有点懂了, 把log以及storage里面的几个辅助函数实现了之后世界美好了好多,唉,没想到这两个文件提供了这么多有用的函数
	prevLogIndex := r.Prs[to].Next-1				// 将要发送给对方的log的前一条log的下标以及任期号,用来让对方比较新旧
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	// 增加了错误处理
	if err != nil {
		if err == ErrCompacted {
			// 出现这种错误就是因为这个follower的日志太老了,它的下一个需要的字段在leader里面已经被compacted了
			// 也就是说storage里面没有这个entry了,所以只能发snapshot给它了
//			log.Infof("[%d] 向 %d 发送了一个 snapshot", r.id, to)
			r.sendSnapShot(to)
			return true
		}

		// 这个错误处理帮大忙了啊, 2B会概率性的出现这个panic
		panic(err)
	}
	msg := pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              prevLogTerm, // Note: 现在非常担心这里可能出现下标越界
		Index:                prevLogIndex,
		Entries:              nil,
		Commit:               r.RaftLog.committed,	// 让对方知道哪些条目是已经可以提交的了
	}
	// 判断自己的日志里面是否有可以添加的
	// DPrintf("len: %d", len(r.RaftLog.entries))
	if r.Prs[to].Next <= r.RaftLog.LastIndex() {
		// DPrintf("正在添加日志条目...")
		// BUG here: index并不是这一个日志在entries中的下标, 太坑了, 所以导致这里appendEntry了个寂寞
		//for _, entry := range r.RaftLog.entries[r.Prs[to].Next:] {
		//	msg.Entries = append(msg.Entries, &entry)
		//}
		DPrintf("next: %d", r.Prs[to].Next)
		// DPrintf("r.RaftLog.entries[r.Prs[to].Next-stabled]: %v", r.RaftLog.entries[r.Prs[to].Next-stabled])
		// update: 做出了修改, 之前对log entry的结构理解有误
		first := r.RaftLog.entries[0].Index
		// fix bug: 这个bug藏得太深了, 注意这个entries数组放的是地址,并且都是entry的地址,导致其实整个数组的值都是一样的,因为地址都一样
		//for _, entry := range r.RaftLog.entries[r.Prs[to].Next-first:] {
		//	DPrintf("eee %v", entry)
		//	msg.Entries = append(msg.Entries, &entry)
		//}
		// DPrintf("r.Prs[to].Next-first: %d, r.RaftLog.LastIndex()-first: %d", r.Prs[to].Next-first, r.RaftLog.LastIndex()-first)
		// 万万没想到, 我竟然在2b把2a的bug找出来了,一个非常大的感悟, bug越早发现越好,否则越到后面调试越困难
		// update: 我裂开了,这里虽然之前发现了bug,但是fix的时候不够仔细,还是错了
		//		   导致后来出现了cb.callback的时候一直说errLeader
		for i := r.Prs[to].Next-first; i <= r.RaftLog.LastIndex()-first; i++ {
			msg.Entries = append(msg.Entries, &r.RaftLog.entries[i])
		}
		DPrintf("[%d] 发送 entries 给 [%d]: %v", r.id, to, msg.Entries)
	}
	r.msgs = append(r.msgs, msg)
	DPrintf("[%d] [Term: %d] 向 [%d] 发送AppendEntry包, entries: %v", r.id, r.Term, to, msg.Entries)
	return true
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
		Commit:               0,
		Reject:               false,
	}
	r.msgs = append(r.msgs, msg)
	DPrintf("[%d] [Term: %d] 向 [%d] 发送心跳包", r.id, r.Term, to)
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
			DPrintf("[%d] [Term: %d] 心跳超时, 准备广播心跳包", r.id, r.Term)
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
			DPrintf("[%d] [Term: %d] 选举超时, 准备广播选举包", r.id, r.Term)
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
	DPrintf("[%d] [Term %d] 成为Follower, lead: %v", r.id, term, lead)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateFollower
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.reject = 0
	// 标记解除
	r.leadTransferee = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 2aa: 根据论文,目前只用管选举相关
	//  在转变成候选⼈后就⽴即开始选举过程
	DPrintf("[%d] [Term+1 = %d] 成为Candidate", r.id, r.Term+1)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateCandidate
	// 候选人没有leader
	r.Lead = None
	//  ⾃增当前的任期号（currentTerm）
	r.Term += 1
	//  给⾃⼰投票
	r.votes[r.id] = true
	r.Vote = r.id
	//  重置选举超时计时器
	r.electionElapsed = 0
	r.reject = 0
	//  发送请求投票的 RPC 给其他所有服务器
	// r.launchVote()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	DPrintf("[%d] [Term: %d] 成为Leader", r.id, r.Term)
	r.votes = make(map[uint64]bool, 0)
	r.State = StateLeader
	// fix bug: 3A里面要求的, leader的Lead是它自己
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 成为领导人之后立即发送空的心跳包给所有的peers
	// fix bug: 如果这里立即广播心跳包, 会导致这个测试点挂掉TestLeaderStartReplication2AB
	// r.broadcastHeartBeat()
	// 每当成为Leader之后都会更新next和index
	prevLogIndex := r.RaftLog.LastIndex()
	for _, peer := range r.peers {
		if peer == r.id {
			// 按照测试用例 TestProgressLeader2AB, 我们需要在这里更新leader 的match与next
			r.Prs[peer].Match = prevLogIndex+1
			r.Prs[peer].Next = prevLogIndex+2
			continue
		}
		r.Prs[peer].Next = prevLogIndex+1
		r.Prs[peer].Match = 0	// 已经匹配的日志的index,初始化时不知道匹配到哪里, 赋值为0
	}
	// The tests assume that the new elected leader should append a noop entry on its term
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType:            pb.EntryType_EntryNormal,
		Term:                 r.Term,
		Index:                prevLogIndex+1,
		Data: 				  nil,
	})

	// 立即发送append包, 不然会导致添加这条新的日志之后,2aa的一些测试点挂掉
	// 这里广播包的时候没有考虑只有一台机器的情况, TestLeaderAcknowledgeCommit2AB这个点挂掉了
	r.broadcastAppendEntries()
}

// 候选人发起选举
func (r *Raft) launchVote () {
	// var peer uint64
	if len(r.peers) == 1 {
		r.becomeLeader()
		return
	}
	r.reject = 0
	for _, peer := range r.peers {
		// fix bug: 不能跳过自己, 有一个毒瘤测试数据,就是单节点的, 如果跳过自己就收不到回信, 那么就无法执行对应的response逻辑了
		// fix bug: 后面的测试说明还是得跳过自己
		// 跳过自己
		if peer == r.id {
			// DPrintf("跳过自己")
			continue
		}
		// fix bug: 这里的lastIndex不应该调用storage里面实现的那个方法, 因为那个只能查出持久化了的最后一个条目的index
		// 			而我在raftlog里面实现的却可以查出所有的条目,包含未提交的里面的最后一个条目的index
		prevLogIndex := r.RaftLog.LastIndex()
		prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   peer,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              prevLogTerm,
			Index:                prevLogIndex,
		}
		DPrintf("[%d] [Term: %d] 向 [%d] 发送了一条 选举请求, prevLogTerm: %d prevLogIndex: %d",
			r.id, r.Term, peer, prevLogTerm, prevLogIndex)
		r.msgs = append(r.msgs, msg)
	}
}

// 广播心跳包
func (r *Raft) broadcastHeartBeat() {
	// 遍历map获得key value, key就是我们想要的东西
	DPrintf("[%d] [Term: %d] 正在广播 心跳包", r.id, r.Term)
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
	DPrintf("[%d] [Term: %d] 正在处理来自 [%d] [Term: %d] 的选举请求", r.id, r.Term, m.From, m.Term)
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
	// DPrintf("run here2\n")

	// length := uint64(len(r.RaftLog.entries))

	// fix bug: 这里需要先判断任期号, 然后再判断日志新旧等决定是否拒绝, 不然这个测试点会挂掉 TestDuelingCandidates2AB
	//   就是一个掉线很久一直自旋选举的机器, 它的任期号很高, 但是日志很旧, 后来重连了, 它向leader请求投票时, leader会变成follower
	//   但是leader会拒绝投票, 因为它的日志太旧了, 之后再次选举时, 这个掉线很久的机器不会当选, leader只会在之前的集群当中产生
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// it votes for the
	//	sender only when sender's last term is greater than MessageType_MsgRequestVote's term or
	//	sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	//	index is greater than or equal to follower's.
	//! TODO: BUG here
	// 修正了这里的判断条件, 添加日志之后,当且仅当候选人任期号比自己大, 或者候选人任期号和自己一样大但是候选人的日志至少
	// 和自己一样新, 一样新指的是最后一条日志的Index至少相同
	// modify: 这里获取最后一个条目的index采用了之前在raftlog里面实现的方法,不用担心stabled里面的条目为空的情况了
	//  		同时term的获取方法也改为了采用之前封装好的方法来进行获取
	prevLogIndex := r.RaftLog.LastIndex()
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	// 比较日志的新旧
	DPrintf("[%d] LastLog: [Trem: %d] [index: %d] logs:%v, [%d].LastLog: [Term: %d] [index: %d]",
		r.id, prevLogTerm, prevLogIndex, r.RaftLog.entries, m.From, m.LogTerm, m.Index)
	if  prevLogTerm> m.LogTerm ||
		((prevLogTerm == m.LogTerm) && m.Index < prevLogIndex) {
		// 拒绝
		DPrintf("m.index %d index %d", m.Index, prevLogIndex)
		msg.Index = prevLogIndex
		msg.LogTerm = prevLogTerm
		r.msgs = append(r.msgs, msg)
		DPrintf("[%d] 拒绝了 [%d] 的选举请求, 因为日志太旧了", r.id, m.From)
		return
	}

	if r.Vote == None || r.Vote == m.From {
		r.Vote = m.From
		msg.Reject = false
		DPrintf("[%d] 同意了 [%d] 的选举请求", r.id, m.From)
	} else {
		DPrintf("[%d] 拒绝了 [%d] 的选举请求, 因为已经给别人投票了", r.id, m.From)
	}
	r.msgs = append(r.msgs, msg)
}

// 处理选举请求的回信
func (r *Raft) handleVoteResponse(m pb.Message) {
	DPrintf("[%d] [Term: %d] 收到了 [%d] [Term: %d] 的投票回信", r.id, r.Term, m.From, m.Term)
	DPrintf("lastIndex: %d m.index: %d", r.RaftLog.LastIndex(), m.Index)
	length := len(r.peers)
	if m.Reject == false {
		// 这个人向我们投票了,那么votes里面这个人置为true
		r.votes[m.From] = true
		// 然后计算总人数是否过半
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
	} else  {
		if m.Term > r.Term{ /*|| (m.Term == r.Term && (m.LogTerm > r.RaftLog.LastTerm() ||
			(m.LogTerm == r.RaftLog.LastTerm() &&  m.Index > r.RaftLog.LastIndex())))*/
			// TestLeaderElectionOverwriteNewerLogs2AB这个测试用例挂了, 仔细分析测试用例之后发现是因为在投票被拒绝之后
			// 	我没有加上是否变为follower的判断逻辑
			// update: 上面的说法错了, 论文遗漏了一点, 就是反对消息超过半数会直接变成follower
			r.becomeFollower(m.Term, None)
		}
		// 暴力出奇迹, 直接加一个字段计算这个值, 我不讲武德的
		r.reject++
		if r.reject > length/2 {
			// fix bug: 这里被拒绝太多时, 成为folower的term不是由m.term决定的, 因为可能自己任期号比较大, 是因为日志
			// 太旧被拒绝的
			r.becomeFollower(r.Term, None)
		}
	}
}

// 处理心跳包的回信
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, None)
	} else {
		// 心累, 这个2A的bug在2C当中才找出来, 找了一天
		r.sendAppend(m.From)
	}
}

// 广播AppendEntry包
//!doc then calls 'bcastAppend' method to send those entries to its peers
func (r *Raft) broadcastAppendEntries () {
	// fix bug: 单台主机的情况不考虑自己会挂掉, 但是不能直接跳过,因为测试里面过滤掉了自己对自己发的请求
	// 所以做了如下的特判
	if len(r.peers) == 1{
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	// 重置计时器
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) updateCommit() {
	// 如果存在一个满足 N > commitIndex 的 N,并且大多数的 matchIndex[i] ≥ N 成立,并且 log[N].term == currentTerm
	// 成立,那么令 commitIndex 等于这个 N (5.3 和 5.4 节)
	sortArr := make([]int, 0)
	for _, peer := range r.peers {
		// DPrintf("sort addr: %d", r.Prs[peer].Match)
		sortArr = append(sortArr, int(r.Prs[peer].Match))
	}
	sort.Ints(sortArr)
	var newCommitIndex int
	// fix bug: 测试不讲武德, 居然有一个测试点的服务器台数是偶数 TestLeaderOnlyCommitsLogFromCurrentTerm2AB
	if len(sortArr) % 2 == 0 {
		newCommitIndex = sortArr[len(sortArr)/2-1]
	} else {
		newCommitIndex = sortArr[len(sortArr)/2]
	}
	//if uint64(newCommitIndex) > r.RaftLog.committed {
	//	r.RaftLog.committed = uint64(newCommitIndex)
	//}
	term, _ := r.RaftLog.Term(uint64(newCommitIndex))
	// DPrintf("term: %d, r.term %d", term, r.Term)
	// 新的提交更大并且是当前任期的提交,那么可以安全的应用
	// find bug: 我终于知道为什么commit够了但是没有提交了, 这里最后一条日志的term计算出现了错误
	if uint64(newCommitIndex) > r.RaftLog.committed && term == r.Term {
		// fix bug: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
		newCommitIndex = int(min(uint64(newCommitIndex), r.RaftLog.LastIndex()))

		DPrintf("[%d] 新的提交, newcommitIndex=%d", r.id, newCommitIndex)
		r.RaftLog.committed = uint64(newCommitIndex)
		// 更新日志提交记录
		// 从上次apply之后的地方开始提交,一直提交到commit的地方
		for i := r.RaftLog.applied+1; i <= uint64(newCommitIndex); i++ {
			// in here you also need to interact with the upper application by the Storage interface
			// defined in raft/storage.go to get the persisted data like log entries and snapshot.
			// TODO: 把可以安全应用的数据apply
		}
		// 更新已经应用的日志的下标
		// 按照测试,貌似现在不能更新applied
		// r.RaftLog.applied = uint64(newCommitIndex)
		// DPrintf("[%d] 新的apply=%d", r.id, r.RaftLog.applied)

		// TestLeaderSyncFollowerLog2AB 这个测试用例说明当commit被更新之后我们还需要发消息给peers,同步这个消息
		r.broadcastAppendEntries()
	}
}

// 处理AppendEntry的响应包
func (r *Raft) handleAppendEntriesResponse (m pb.Message) {
	if m.Reject {
		DPrintf("[%d] AppendEntried 被 [%d] 拒绝", r.id, m.From)
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return
		}
		// 否则说明是日志被拒绝
		// 失败是因为prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配
		// 按照论文里面说的, 如果因为日志不一致而失败,减少 nextIndex 重试
		r.Prs[m.From].Next = m.Index+1
		// 然后重发, 按照测试TestLeaderSyncFollowerLog2AB的逻辑应该是需要立即重发
		r.sendAppend(m.From)
	} else {
		DPrintf("[%d] AppendEntried 被 [%d] 接收, match: %d", r.id, m.From, m.Index)
		// 否则说明日志被成功接收了, 更新match与next数组
		r.Prs[m.From].Match = m.Index
		// DPrintf("%d, %d", m.Index, r.Prs[r.id].Match)
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		// fix bug: 在计算commit超过半数的机器时, 忘记更新leader自己的match了, 导致出现错误
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.updateCommit()
		// 在最后做一个判断
		if r.leadTransferee == m.From {
			msg := pb.Message{
				MsgType:              pb.MessageType_MsgTransferLeader,
				To:                   r.id,
				From:                 r.leadTransferee,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

//  first check the qualification of the transferee (namely transfer target) like:
//  is the transferee’s log up to date, etc.
func (r *Raft) checkQualification(id uint64) (bool, error) {
	if r.Prs[id] == nil {
		return false, errors.New("not exist")
	}
	if r.Prs[id].Match == r.RaftLog.LastIndex() {
		return true, nil
	}
	return false, nil
}

// 这里有一个比较棘手的问题,就是当transferee不合法的时候,我们需要sendappend, 但是又不能够propose,避免cycle
// 完了之后我们还得继续执行MessageType_MsgTimeoutNow的逻辑,转换身份
// 解决办法,利用raft结构体的leadTransferee字段标记一下
func (r *Raft) handleTransfer(m pb.Message) {
	if r.id == m.From {
		// 自己就是leader, 让位给自己的事情大可不必
		return
	}
	valid, err := r.checkQualification(m.From)
	if err != nil {
		log.Infof("LeaderTransferToNonExistingNode")
		return
	}
	r.leadTransferee = m.From
	if valid {
		// send a MsgTimeoutNow message to the transferee immediately
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgTimeoutNow,
			To:                   m.From,
			From:                 r.id,
		}
		r.msgs = append(r.msgs, msg)
		// fix bug: 这里leader成为follower是通过日志一样新的follower收到MessageType_MsgTimeoutNow之后发起选举
		// 然后收到它的投票之后才会发生身份变更, 而不是自己becomeFollower, TestLeaderTransferBack3A
		// r.becomeFollower(r.Term, m.From)
		log.Infof("[%d] send MessageType_MsgTimeoutNow to %d", r.id, m.From)
	} else {
		// If the transferee’s log is not up to date, the current leader should send a MsgAppend message
		// to the transferee and stop accepting new proposes in case we end up cycling
		r.sendAppend(m.From)
	}
}

// the leader first calls the 'appendEntry' method to append entries to its log
func (r *Raft) appendEntry(m pb.Message) {
	// when TransferLeader stop accepting new proposes in case we end up cycling
	if r.leadTransferee != None {
		// 目前暂时这样处理了, 不知道会不会有问题
		return
	}
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex()+1
		entry.Term = r.Term
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				// 当前raft的pendingconf 还没有被应用,那么忽略这个entry
				log.Infof("当前raft的pendingconf 还没有被应用,那么忽略这个entry")
				return
			} else {
				// 否则更新r.PendingConfIndex
				r.PendingConfIndex = entry.Index
			}
		}
		DPrintf("***appendEntry***\n[%d] [Term: %d] 正在添加新的entry, Index: %d, Term: %d, entry: %v",
			r.id, r.Term, entry.Index, entry.Term, *entry)
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// 每次添加新的entry之后都需要更新自己
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 在2ab卡住了,于是静下心来看了一下文档,发现doc.go写的真是太好了,我好多地方实现的都不够优雅
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 2aa: 框架代码这里的状态机的思想的应用简直太经典了
	//if m.MsgType == pb.MessageType_MsgSnapshot {
	//	log.Infof("[%d] 收到了 snapshot的install请求", r.id)
	//}
	if r.Prs[r.id] == nil && m.MsgType == pb.MessageType_MsgTimeoutNow {
		log.Infof("[%d] has been removed", r.id)
		return nil
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 当存在未被应用的confchange时,拒绝MessageType_MsgHup操作。
			if r.PendingConfIndex > r.RaftLog.applied {
				return nil
			}
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
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.becomeCandidate()
			r.launchVote()
		case pb.MessageType_MsgTransferLeader:
			// 转发
			log.Infof("[%d] %v 转发 MessageType_MsgTransferLeader 给 %d", r.id, r.State, r.Lead)
			msg := pb.Message{
				MsgType:              pb.MessageType_MsgTransferLeader,
				To:                   r.Lead,
				From:                 m.From,
			}
			r.msgs = append(r.msgs, msg)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 当存在未被应用的confchange时,拒绝MessageType_MsgHup操作。
			if r.PendingConfIndex > r.RaftLog.applied {
				return nil
			}
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
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.becomeCandidate()
			r.launchVote()
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.broadcastHeartBeat()
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartBeatResponse(m)
		case pb.MessageType_MsgPropose:
			r.appendEntry(m)
			r.broadcastAppendEntries()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handlerVote(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransfer(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("[%d] [Term: %d] 正在处理来自 [%d] [Term: %d] 的 AppendEntry 包", r.id, r.Term, m.From, m.Term)
	for _, entry := range m.Entries {
		DPrintf("--- 其中的entries: %v", *entry)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject: true,
		Index: m.Index + uint64(len(m.Entries)),
	}
	//  When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
	//	back to follower, because it indicates that there is a valid leader sending
	//	'MessageType_MsgAppend' messages.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msg)
		DPrintf("[%d] 拒绝了 [%d] 的 AppendEntry 包, 因为Term太小了", r.id, m.From)
		return
	}
	// 对于领导人是否做小弟, 还需要看日志索引值的比较结果,但2aa暂未实现log
	// 否则收到了心跳包,老老实实的当小弟
	r.becomeFollower(m.Term, m.From)
	DPrintf("[%d] 接收了来自 [%d] 的 AppendEntry 包, 并且回了信", r.id, m.From)

	// 还得判断任期号是否匹配
	prevLogIndex := r.RaftLog.LastIndex()
	// Note here: may be exist bug, i'm not sure
	// modify: 发现这里其实不需要考虑next一直减减到负数的情况,因为它们的0肯定是相等的, 所以去掉了之前的一个if判断条件
	if prevLogIndex < m.Index {
		DPrintf("[%d] 日志没有那么长,匹配不了", r.id)
		// 没有与之相匹配的日志条目, 返回, 让leader减小next
		msg.Index = prevLogIndex
		r.msgs = append(r.msgs, msg)
		return
	}
	// fix bug: 这里比较的时候比的是m.Index这个下标的任期号, 我已开始是与最后一个任期号作比较的
	prevLogTerm, _ := r.RaftLog.Term(m.Index)
	if prevLogTerm != m.LogTerm {
		// 由于遇到了一个bug, 连续生成snap失败5次, 我以为是我没有实现快速回退导致的, 但是后来发现是其他原因
		l := r.RaftLog
		// 对应处的日志的任期号不匹配,那么leader也得减小next, 这样是为了之后可以把不匹配的部分覆盖删除掉, 日志以leader为准
		index := min(m.Index, l.LastIndex())
		for index > l.committed {
			term, err := l.Term(index)
			if err != nil || term != prevLogTerm {
				break
			}
			index--
		}
		msg.Index = index
//		log.Infof("index %v, prevLogindex %v", index, prevLogIndex)
		r.msgs = append(r.msgs, msg)
		return
	}


	// 如果已经存在的日志条目和新的产生冲突(索引值相同但是任期号不同),删除这一条和之后所有的 (5.3 节)
	// 我的个人理解就是如果rf.log下标为prevLogIndex+1的地方如果存在元素并且term与传过来的entries中的term不同,那么这个以及后面的全部不要
	// fix bug: 这里应该需要一个个的比较,考虑以下情况:
	//         ↓ (prevIndex)
	// logs: 1 1 1 2 2 2 2
	// args:     1 1 2 2 2 2
	// 			   ↑ (term不相等, 之后的都切掉)
	len_ := r.RaftLog.LastIndex() - m.Index
	overlap := 0
	// tt, _ := r.RaftLog.Term(m.Index)
	// 所以如果prevlogIndex 与 prevLogTerm匹配上了之后, 但是entries为空, 那么follower里面匹配的后面的日志是否应该删除呢?
	// 学长答疑: 不应该删, 只有产生冲突才应该删除
	//if len(m.Entries) == 0 && tt == m.LogTerm {
	//	DPrintf("[%d] 收到 [%d] 的 AppendEntries, 为空,且匹配, %d", r.id, m.From, m.Index)
	//	if m.Commit > r.RaftLog.committed {
	//		r.RaftLog.committed = m.Index
	//	}
	//}
	for i := uint64(0); i < len_ && i < uint64(len(m.Entries)); i++ {
		term, _ := r.RaftLog.Term(m.Index+i+1)
		// fix bug: 这里下标越界了好多次, 目前还不能肯定这里的逻辑是不是对的
		if term != m.Entries[i].Term && m.Index+i+1 > r.RaftLog.committed {
			// fix bug: 这里截断时的下标操作一开始搞错了
			first := r.RaftLog.entries[0].Index
			r.RaftLog.entries = r.RaftLog.entries[:m.Index+i+1-first]
			DPrintf("[%d] 收到 [%d] 的 AppendEntries 之后删除了不匹配的日志条目,剩余的为 %v", r.id, m.From, r.RaftLog.entries)
			// fix bug: 截断之后,stabled可能还需要进行改变, 因为storage里面的数据可能出现被截断的情况,这个在测试用例里面出现了
			//				TestFollowerCheckMessageType_MsgAppend2AB
			if r.RaftLog.stabled > m.Index+i {
				DPrintf("[%d] 更新了 stabled: before %d after %d", r.id, r.RaftLog.stabled, m.Index+i)
				r.RaftLog.stabled = m.Index+i
			}
			break
		}
		overlap++
	}

	// 添加日志
	for _, entry := range m.Entries[overlap:] {
		DPrintf("[%d] 在log当中添加了条目 %v ", r.id, *entry)
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	// 然后判断是否需要commit
	if m.Commit > r.RaftLog.committed {
		// 需要commit了
		// fix bug: 之前更新commit的操作完全错误 TestHandleMessageType_MsgAppend2AB ensure3
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		for i := r.RaftLog.applied+1; i <= r.RaftLog.committed; i++ {
			// TODO: apply
			DPrintf("[%d] apply", r.id)
		}
	}

	// 回信
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("[%d] [Term: %d] 正在处理来自 [%d] [Term: %d] 的心跳包", r.id, r.Term, m.From, m.Term)
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
	//  follower should call handleSnapshot to handle it, which namely just restore
	//  the raft internal state like term, commit index and membership information, ect,
	//  from the eraftpb.SnapshotMetadata in the message
//	log.Infof("[%d] [Term: %d] 正在处理来自 [%d] [Term: %d] 的snapshot", r.id, r.Term, m.From, m.Term)
	if m.Snapshot == nil {
		log.Infof("!!! snapshot is nil")
		return
	}
	// 代码结构参考了etcd的实现
	if r.restore(*m.Snapshot) {
		r.becomeFollower(m.Term, m.From)
		// 哇, 为什么没有早点知道etcd, 它的结构设计的真好, 我这样发消息搞得代码好臃肿
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			Index:                r.RaftLog.LastIndex(),
			Reject:               false,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			Index:                r.RaftLog.committed,
			Reject:               false,
		}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		// 如果现存的⽇志条⽬与快照中最后包含的⽇志条⽬具有相同的索引值和任期号，则保留其后的⽇志 条⽬并进⾏回复
		// 出现这种情况说明快照里面的东西这台机器都存了
		// bug: panic: runtime error: index out of range [0] with length 0
		//if r.RaftLog.entries[0].Index <= s.Metadata.Index {
		//	// 仅保留其后的⽇志
		//	r.RaftLog.entries = r.RaftLog.entries[s.Metadata.Index-r.RaftLog.entries[0].Index+1:]
		//}
		return false
	}
	// 保存快照⽂件，丢弃具有较⼩索引的任何现有或部分快照
	r.RaftLog.pendingSnapshot = &s
//	log.Infof("我这里确实保存了快照文件: %v", r.RaftLog.pendingSnapshot)
	// fix bug: 这里不如不丢弃的话, 由于这些日志是已经
	// 否则的话丢弃整个⽇志
	r.RaftLog.entries = nil
	// 使⽤快照重置状态机
	meta := s.Metadata
	// restore the raft internal state like term, commit index and membership information
	// 内部仅更新了 committed，并没有更新 applied。这是因为 raft-rs 仅关心 Raft 日志的部分，
	// 至于如何把日志中的内容更新到真正的状态机中，是应用程序的任务。(官方博客)
	// r.RaftLog.applied = meta.Index // fix bug here
	r.RaftLog.committed = meta.Index

	// （并加载快照的集群配置） membership information
	// 因为 Snapshot 包含了 Leader 上最新的信息，而 Leader 上的 Configuration 是可能跟 Follower 不同的。
	r.Prs = make(map[uint64]*Progress)
	r.peers = nil
	for _, peer := range meta.ConfState.Nodes {
		r.peers = append(r.peers, peer)
		r.Prs[peer] = &Progress{}
	}
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("[%d] addNode %v", r.id, id)
	flag := false
	for _, peer := range r.peers {
		if peer == id {
			flag = true
			break
		}
	}
	if !flag {
		r.peers = append(r.peers, id)
		r.Prs[id] = &Progress{
			Match: 0,
			// fix bug: 一个新加入的节点的初始logindex是0, 那么下一条要发给他的就是1
			Next:  1,
		}
	}
	r.PendingConfIndex = 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
//	log.Infof("[%d] remove %v", r.id, id)
	// Your Code Here (3A).
	for index, peer := range r.peers {
		if peer == id {
			r.peers = append(r.peers[:index], r.peers[index+1:]...)
			delete(r.Prs, peer)
			break
		}
	}
	// fix bug: TestCommitAfterRemoveNode3A这个测试挂掉了, 这个测试检测的就是集群成员变化提交之后, 由于节点数发生了变化
	// 有一些日志也许就可以提交了, 所以我们得重新判断一下
	if r.State == StateLeader {
		r.updateCommit()
	}
	r.PendingConfIndex = 0
}
