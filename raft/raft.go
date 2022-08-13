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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
	}
	hardState, confState, _ := c.Storage.InitialState()
	r.Vote, r.Term, r.RaftLog.committed = hardState.Vote, hardState.Term, hardState.Commit
	if c.peers == nil {
		c.peers = confState.GetNodes()
	}
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: r.RaftLog.LastIndex()}
		} else {
			r.Prs[peer] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}
		//r.votes[peer] = false
	}
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	r.becomeFollower(0, None)
	r.RaftLog.applied = c.Applied

	// leadTransferee 和 PendingConfIndex在3A补上
	return r
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			// 选举间隔要设置为一个随机数，避免多个节点同时发起选举
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			// 转化为候选者角色后开启选举过程
			r.becomeCandidate()
			r.startRequestVote()
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			// 重新开始选举
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.electionElapsed += 1
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 发送心跳信息
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	//for k, _ := range r.votes {
	//	r.votes[k] = false
	//}
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	//for peer, _ := range r.votes {
	//	if peer == r.id {
	//		r.votes[peer] = true
	//	} else {
	//		r.votes[peer] = false
	//	}
	//}
	r.votes[r.id] = true
	r.Vote = r.id
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	r.heartbeatElapsed = 0

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIndex + 1})
	if len(r.Prs) == 1 {
		r.RaftLog.committed += 1
		return
	}
	for peer, _ := range r.Prs {
		if peer == r.id {
			r.Prs[peer] = &Progress{lastIndex + 1, lastIndex + 2}
		} else {
			r.Prs[peer] = &Progress{0, lastIndex + 1}
			r.sendAppend(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.startRequestVote()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.Term += 1
			r.startRequestVote()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.startSendHeartbeat()
		case pb.MessageType_MsgPropose:
			r.propose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

func (r *Raft) propose(m pb.Message) {
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	for peer, _ := range r.Prs {
		if r.id == peer {
			pr := r.Prs[r.id]
			pr.Match += 1
			pr.Next += 1
			r.Prs[r.id] = pr
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

func (r *Raft) startRequestVote() {
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer)
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	lastterm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastterm,
		Index:   lastIndex}
	r.msgs = append(r.msgs, msg)
}

// 处理来自candidate的投票RPC请求
func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			Reject:  true}
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = None
	}
	reject := true
	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		// candidate的log比自己的log更新才能对candidate进行投票
		if (lastTerm < m.LogTerm) || (lastTerm == m.LogTerm && lastIndex <= m.Index) {
			r.Vote = m.From
			reject = false
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = None
		return
	}
	if m.Term == r.Term && m.Reject == false {
		r.votes[m.From] = true
	}
	if m.Term == r.Term && m.Reject == true {
		r.votes[m.From] = false
	}
	if r.State == StateLeader {
		return
	}
	voteNum := 0
	rejectNum := 0
	for peer, v := range r.votes {
		if peer == r.id {
			voteNum += 1
			continue
		}
		if v == true {
			voteNum += 1
		} else {
			rejectNum += 1
		}
	}
	if voteNum > len(r.Prs)/2 {
		r.becomeLeader()
	}
	if rejectNum > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	progress := r.Prs[to]
	next := progress.Next
	preveLogIndex := next - 1
	preveLogTerm, _ := r.RaftLog.Term(preveLogIndex)
	// 发送的log entries
	entries_ := make([]pb.Entry, 0)
	if next <= r.RaftLog.LastIndex() {
		first := r.RaftLog.entries[0].Index
		if next < first {
			return false
		}
		entries_ = r.RaftLog.entries[next-first:]
	}
	entries := make([]*pb.Entry, len(entries_))
	for idx := 0; idx < len(entries_); idx++ {
		entries[idx] = &entries_[idx]
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: entries,
		LogTerm: preveLogTerm,
		Index:   preveLogIndex,
		Commit:  r.RaftLog.committed}
	r.msgs = append(r.msgs, msg)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			Reject:  true}
		r.msgs = append(r.msgs, msg)
		return
	}

	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Lead = m.From
		r.Vote = None
	}
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

	preveTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || preveTerm != m.LogTerm {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			Reject:  true}
		r.msgs = append(r.msgs, msg)
		return
	}
	//mStorage, _ := r.RaftLog.storage.(*MemoryStorage)
	if len(m.Entries) > 0 {
		if len(r.RaftLog.entries) > 0 {
			for i, entry := range m.Entries {
				index := entry.Index
				term := entry.Term
				if t, err := r.RaftLog.Term(index); err != nil || term != t {
					offset := r.RaftLog.entries[0].Index
					r.RaftLog.entries = r.RaftLog.entries[:index-offset]
					// There is only updating stabled but not storage
					if r.RaftLog.stabled >= index {
						entries := []pb.Entry{}
						for _, e := range r.RaftLog.entries {
							if e.Index == index-1 {
								entries = append(entries, e)
								break
							}
						}
						r.RaftLog.stabled = index - 1
						storage := r.RaftLog.storage.(*MemoryStorage)
						storage.Append(entries)
					}
					for idx := i; idx < len(m.Entries); idx++ {
						r.RaftLog.entries = append(r.RaftLog.entries, *(m.Entries[idx]))
					}
					break
				}
			}
		} else {
			for _, entry := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}

	}
	m.Commit = min(m.Commit, m.Index+uint64(len(m.Entries)))
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		Index:   m.Index,
		Entries: m.Entries,
		Reject:  false}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if m.Reject {
		if r.Prs[m.From].Next > 1 {
			next := min(r.Prs[m.From].Next-1, r.RaftLog.LastIndex()+1)
			progress := &Progress{Match: r.Prs[m.From].Match, Next: next}
			r.Prs[m.From] = progress
		}
		r.sendAppend(m.From)
	} else {
		progress := &Progress{}
		progress.Match = min(m.Index+uint64(len(m.Entries)), r.RaftLog.LastIndex())
		progress.Next = progress.Match + 1
		r.Prs[m.From] = progress
		for i := progress.Match; i > r.RaftLog.committed; i-- {
			// Figure 8
			term, _ := r.RaftLog.Term(i)
			if term != r.Term {
				break
			}
			num := 1
			for peer, _ := range r.Prs {
				if peer == r.id || r.Prs[peer].Match < i {
					continue
				}
				if r.Prs[peer].Match >= i {
					num += 1
				}
			}
			if num > len(r.Prs)/2 {
				r.RaftLog.committed = i
				for peer, _ := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
				break
			}
		}
	}
}

func (r *Raft) startSendHeartbeat() {
	for peer, _ := range r.Prs {
		if r.id == peer {
			continue
		}
		r.sendHeartbeat(peer)
	}
	r.heartbeatElapsed = 0
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Commit:  r.RaftLog.committed,
		Term:    r.Term}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    m.To,
			To:      m.From,
			Term:    r.Term}
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.Vote = None
	}
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	reject := false
	if r.RaftLog.committed != m.Commit {
		reject = true
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = None
	}
	r.heartbeatElapsed = 0
	if m.Reject {
		r.sendAppend(m.From)
	}
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
