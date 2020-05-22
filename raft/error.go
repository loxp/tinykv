package raft

import "errors"

var (
	ErrorOverdueTerm = errors.New("overdue term")
)
