package raft

import "errors"

var (
	ErrOverdueTerm    = errors.New("overdue term")
	ErrInvalidLogTerm = errors.New("invalid log term")
)
