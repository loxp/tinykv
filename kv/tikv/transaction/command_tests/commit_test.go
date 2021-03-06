package command_tests

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

// TestEmptyCommit4A tests a commit request with no keys to commit.
func TestEmptyCommit4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([][]byte{}...)
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 0)
}

// TestSimpleCommit4A tests committing a single key.
func TestSingleCommit4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitOverwrite4A tests committing where there is already a write.
func TestCommitOverwrite4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		// A previous, committed write.
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// The current, pre-written write.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitMultipleKeys4A tests committing multiple keys in the same commit. Also puts some other data in the DB and test
// that it is unchanged.
func TestCommitMultipleKeys4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3}, []byte{12, 4, 0}, []byte{15})
	builder.init([]kv{
		// Current, pre-written.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{12, 4, 0}, value: []byte{1, 1, 0, 0, 1, 5}},
		{cf: engine_util.CfLock, key: []byte{12, 4, 0}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{15}, value: []byte{0}},
		{cf: engine_util.CfLock, key: []byte{15}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},

		// Some committed data.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80, value: []byte{150}},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},

		// Another pre-written transaction.
		{cf: engine_util.CfDefault, key: []byte{2}, ts: 99, value: []byte{0, 0, 0, 8}},
		{cf: engine_util.CfLock, key: []byte{2}, value: []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0}},
		{cf: engine_util.CfDefault, key: []byte{43, 6}, ts: 99, value: []byte{1, 1, 0, 0, 1, 5}},
		{cf: engine_util.CfLock, key: []byte{43, 6}, value: []byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(7, 2, 5)
	builder.assert([]kv{
		// The newly committed data.
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{12, 4, 0}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfWrite, key: []byte{15}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},

		// Committed data is untouched.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84},

		// Pre-written data is untouched.
		{cf: engine_util.CfDefault, key: []byte{2}, ts: 99},
		{cf: engine_util.CfLock, key: []byte{2}},
		{cf: engine_util.CfDefault, key: []byte{43, 6}, ts: 99},
		{cf: engine_util.CfLock, key: []byte{43, 6}},
	})
}

// TestRecommitKey4A tests committing the same key multiple times in one commit.
func TestRecommitKey4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3}, []byte{3})
	builder.init([]kv{
		// The current, pre-written write.
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, builder.ts(), 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitConflictRollback4A tests committing a rolled back transaction.
func TestCommitConflictRollback4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{3, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(0, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
	})
}

// TestCommitConflictRace4A tests committing where a key is pre-written by a different transaction.
func TestCommitConflictRace4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 90, value: []byte{110}},
		{cf: engine_util.CfLock, key: []byte{3}, value: []byte{1, 3, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, 0}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.NotNil(t, resp.Error.Retryable)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 1, 0)
	builder.assert([]kv{
		{cf: engine_util.CfLock, key: []byte{3}},
		{cf: engine_util.CfDefault, key: []byte{3}, ts: 90},
	})
}

// TestCommitConflictRepeat4A tests recommitting a transaction (i.e., the same commit request is received twice).
func TestCommitConflictRepeat4A(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		{cf: engine_util.CfDefault, key: []byte{3}, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, builder.ts()}},
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(1, 0, 1)
	builder.assert([]kv{
		{cf: engine_util.CfWrite, key: []byte{3}, ts: 110},
		{cf: engine_util.CfDefault, key: []byte{3}},
	})
}

// TestCommitMissingPrewrite4a tests committing a transaction which was not prewritten (i.e., a request was lost, but
// the commit request was not).
func TestCommitMissingPrewrite4a(t *testing.T) {
	builder := newBuilder(t)
	cmd := builder.commitRequest([]byte{3})
	builder.init([]kv{
		// Some committed data.
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80, value: []byte{15}},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80, value: []byte{150}},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 80}},
		// Note no prewrite.
	})
	resp := builder.runOneRequest(cmd).(*kvrpcpb.CommitResponse)

	assert.Nil(t, resp.Error)
	assert.Nil(t, resp.RegionError)
	builder.assertLens(2, 0, 2)
	builder.assert([]kv{
		{cf: engine_util.CfDefault, key: []byte{4}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{4}, ts: 84},
		{cf: engine_util.CfDefault, key: []byte{3, 0}, ts: 80},
		{cf: engine_util.CfWrite, key: []byte{3, 0}, ts: 84},
	})
}

func (builder *testBuilder) commitRequest(keys ...[]byte) *kvrpcpb.CommitRequest {
	var req kvrpcpb.CommitRequest
	req.StartVersion = builder.nextTs()
	req.CommitVersion = builder.prevTs + 10
	req.Keys = keys
	return &req
}
