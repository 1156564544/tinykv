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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
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
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	snapshot, _ := storage.Snapshot()
	log := &RaftLog{}
	log.storage = storage
	log.committed = snapshot.Metadata.Index
	log.applied = log.committed
	log.stabled, _ = storage.LastIndex()

	firstindex, _ := storage.FirstIndex()
	lastindex, _ := storage.LastIndex()
	entries, err := storage.Entries(firstindex, lastindex+1)
	if err != nil {
		log.entries = make([]pb.Entry, 0)
	} else {
		log.entries = entries
	}
	// 快照这一块2C再酌情补充
	log.pendingSnapshot = &snapshot
	return log
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
	ans := make([]pb.Entry, 0)
	for _, entry := range l.entries {
		if entry.Index > l.stabled {
			ans = append(ans, entry)
		}
	}
	return ans
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed == l.applied {
		return
	}
	first, _ := l.storage.FirstIndex()

	if l.applied < first {
		ents, _ = l.storage.Entries(first, l.committed+1)
	} else {
		ents, _ = l.storage.Entries(l.applied, l.committed+1)
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	n := len(l.entries)
	if n > 0 {
		return l.entries[n-1].Index
	}
	last, _ := l.storage.LastIndex()
	return last
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l.LastIndex() < i {
		errorInfor := fmt.Sprintf("The term %v is greater than the last index %v\n", i, l.LastIndex())
		return 0, errors.New(errorInfor)
	}
	if i > l.stabled {
		for _, entry := range l.entries {
			if entry.Index == i {
				return entry.Term, nil
			}
		}
	}
	return l.storage.Term(i)
}
