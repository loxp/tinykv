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
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	electionTick    int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{}

	// init raft with state follower
	r.State = StateFollower

	// init with config
	r.id = c.ID
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTick = c.ElectionTick
	r.setRandomElectionTimeout()
	r.RaftLog = newLog(c.Storage)

	// use prs to store peers
	r.Prs = make(map[uint64]*Progress)
	for _, peerId := range c.peers {
		r.Prs[peerId] = &Progress{}
	}

	// init vote info
	r.resetVoteInfo()

	return r
}

func (r *Raft) setRandomElectionTimeout() {
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick+1)
}

func (r *Raft) resetVoteInfo() {
	r.Vote = None
	r.votes = make(map[uint64]bool)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var unstableEntryPtrs []*pb.Entry
	for _, entry := range r.RaftLog.unstableEntries() {
		unstableEntryPtrs = append(unstableEntryPtrs, &entry)
	}
	msg := pb.Message{
		Term:    r.Term,
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgAppend,
		Entries: unstableEntryPtrs,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		Term:    r.Term,
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		Term:    r.Term,
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgRequestVote,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.isElectionTimeout() {
			// election timeout, go to election
			r.changeStateToCandidateAndAction()
			return
		}
	case StateCandidate:
		r.electionElapsed++
		if r.isElectionTimeout() {
			r.changeStateToCandidateAndAction()
			return
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.sendHeartbeatMessagesToOthers()
		}
	}
}

func (r *Raft) changeStateToCandidateAndAction() {
	r.becomeCandidate()
	r.candidateRequestVote()
}

func (r *Raft) isElectionTimeout() bool {
	//timeout := r.electionTimeout + rand.Intn(r.electionTimeout+1)
	//if r.electionElapsed >= timeout {
	//	fmt.Printf("election timeout: %d\n", timeout)
	//	return true
	//} else {
	//	return false
	//}
	//return r.electionElapsed >= timeout
	return r.electionElapsed >= r.electionTimeout
}

func (r *Raft) voteForSelf() {
	r.Vote = r.id
	r.votes[r.id] = true
}

func (r *Raft) sendRequestVoteToOthers() {
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendRequestVote(peerId)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.resetVoteInfo()
	r.setRandomElectionTimeout()
	r.resetElectionTimer()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.increaseTerm()
	r.resetVoteInfo()
	r.voteForSelf()
	r.setRandomElectionTimeout()
	r.resetElectionTimer()
}

func (r *Raft) candidateRequestVote() {
	if len(r.Prs) == 1 {
		// only one peer, and this peer is myself, of course I win~
		if _, ok := r.Prs[r.id]; ok {
			// take care!
			r.becomeLeader()
			return
		} else {
			panic(errors.New("why do I start up"))
		}
	} else {
		r.sendRequestVoteToOthers()
	}
}

func (r *Raft) increaseTerm() {
	r.Term++
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	r.setRandomElectionTimeout()
	r.resetElectionTimer()
	r.heartbeatElapsed = 0
	r.resetVoteInfo()
	// NOTE: Leader should propose a noop entry on its term
	r.sendHeartbeatMessagesToOthers()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// message term should not be less than current term
	if !IsLocalMsg(m.MsgType) && m.Term < r.Term {
		return ErrOverdueTerm
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return r.stepLocalMsgHup(m)
	case pb.MessageType_MsgRequestVote:
		return r.stepRequestVote(m)
	case pb.MessageType_MsgPropose:
		return r.stepLocalPropose(m)
	case pb.MessageType_MsgAppend:
		return r.stepAppend(m)
	case pb.MessageType_MsgHeartbeat:
		return r.stepHeartbeat(m)
	case pb.MessageType_MsgAppendResponse:
		return r.handleAppendResponse(m)
	default:
		return r.handleDefaultMsg(m)
	}
}

func (r *Raft) stepLocalMsgHup(m pb.Message) error {
	if m.From == r.id && m.To == r.id {
		if r.State != StateLeader { //TestCampaignWhileLeader2AA
			r.changeStateToCandidateAndAction()
		}
		return nil
	}
	return ErrInvalidMsgHup
}

func (r *Raft) stepRequestVote(m pb.Message) error {
	if m.Term >= r.Term {
		if m.Term > r.Term {
			// TestVoteFromAnyState2AA
			r.State = StateFollower
			r.Term = m.Term
			r.resetVoteInfo()
		}

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			return ErrInvalidLogTerm
		}
		if m.LogTerm < lastTerm {
			r.sendRequestVoteResponse(m.From, m.Term, true)
			return nil
		} else if m.LogTerm > lastTerm {
			reject := r.handleVoteFor(m.From)
			r.sendRequestVoteResponse(m.From, m.Term, reject)
			return nil
		} else {
			if m.Index >= lastIndex {
				reject := r.handleVoteFor(m.From)
				r.sendRequestVoteResponse(m.From, m.Term, reject)
				return nil
			} else {
				r.sendRequestVoteResponse(m.From, m.Term, true)
				return nil
			}
		}
	} else {
		r.sendRequestVoteResponse(m.From, m.Term, true)
		return nil
	}
}

func (r *Raft) sendRequestVoteResponse(to uint64, term uint64, reject bool) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    term,
		Reject:  reject,
	}
	r.pushMessageToSend(resp)
}

func (r *Raft) pushMessageToSend(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) isVoteWin() bool {
	return len(r.votes) > len(r.Prs)/2
}

func (r *Raft) handleVoteFor(id uint64) (reject bool) {
	if r.Vote == id {
		return false
	}
	if r.Vote != None {
		return true
	}
	r.Vote = id
	return false
}

func (r *Raft) sendHeartbeatMessagesToOthers() {
	for peerId := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendHeartbeat(peerId)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.manageStateTypeFromAppendAndHeartbeatMsg(m)

	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		//LogTerm: r.RaftLog.Term(r.RaftLog.LastIndex()),
		//Index:   r.RaftLog.LastIndex(),
	}

	r.pushMessageToSend(resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Index:   r.RaftLog.LastIndex(),
	}
	r.pushMessageToSend(resp)
	r.manageStateTypeFromAppendAndHeartbeatMsg(m)
}

func (r *Raft) manageStateTypeFromAppendAndHeartbeatMsg(m pb.Message) {
	switch r.State {
	case StateLeader:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.resetElectionTimer()
		} else {
			// do nothing
		}
	case StateFollower:
		if m.Term > r.Term {
			r.Term = m.Term
			r.resetVoteInfo()
			r.resetElectionTimer()
			r.RaftLog.committed = m.Commit
		} else if m.Term == r.Term {
			r.resetElectionTimer()
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = m.Commit
			}
		} else {
			// do nothing
		}
	case StateCandidate:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.RaftLog.committed = m.Commit
		}
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

func (r *Raft) stepLocalPropose(m pb.Message) error {
	for peerId := range r.Prs {
		r.sendAppend(peerId)
	}
	return nil
}

func (r *Raft) handleAppendResponse(m pb.Message) error {
	return nil
}

func (r *Raft) stepAppend(m pb.Message) error {
	r.handleAppendEntries(m)
	return nil
}

func (r *Raft) stepHeartbeat(m pb.Message) error {
	r.handleHeartbeat(m)
	return nil
}

func (r *Raft) handleDefaultMsg(m pb.Message) error {
	switch r.State {
	case StateFollower:
		if m.Term > r.Term {
			r.Term = m.Term
			// something else need to do
		}
	case StateCandidate:
		switch m.MsgType {
		//case pb.MessageType_MsgAppend:
		//	fallthrough
		//case pb.MessageType_MsgHeartbeat:
		//	r.becomeFollower(m.Term, m.From)
		//	return nil
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Term == r.Term && m.To == r.id && !m.Reject {
				r.votes[m.From] = true
				if r.isVoteWin() {
					r.becomeLeader()
				}
			}
		}
	case StateLeader:
		if m.Term > r.Term {
			r.Term = m.Term
			r.State = StateFollower
			fmt.Printf("[Step] leader %d change to follower, term: %d, %d\n", r.id, r.Term, m.Term)
			// TODO: handle this message
			return nil
		}
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			if len(m.Entries) == 0 {
				// broadcast a heartbeat message to all peers
				r.sendHeartbeatMessagesToOthers()
			}
		case pb.MessageType_MsgBeat:
			r.sendHeartbeatMessagesToOthers()
		}
	}
	return nil
}
