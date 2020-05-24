package raft

import "errors"

var (
	ErrOverdueTerm      = errors.New("overdue term")
	ErrInvalidLogTerm   = errors.New("invalid log term")
	ErrInvalidMsgHup    = errors.New("invalid msg hup")
	ErrInvalidRaftState = errors.New("invalid raft state")
	ErrCannotHandleMsg  = errors.New("cannot handle this type message")
)
