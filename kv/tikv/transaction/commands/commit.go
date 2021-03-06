package commands

import (
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/tikv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	startTs := c.request.StartVersion
	if commitTs <= startTs {
		return nil, fmt.Errorf("invalid transaction timestamp: %d (commit TS) <= %d (start TS)", commitTs, startTs)
	}

	response := new(kvrpcpb.CommitResponse)
	txn.StartTS = &startTs

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return regionError(err, response)
	}
	if lock == nil {
		return nil, nil
	}

	if lock.Ts != *txn.StartTS {
		// Key is locked by a different transaction.
		write, _, err := txn.FindWrite(key, *txn.StartTS)
		if err != nil {
			return regionError(err, response)
		}
		if write == nil || write.Kind == mvcc.WriteKindRollback {
			// Transaction has been rolled back.
			respValue := reflect.ValueOf(response)
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
			return response, nil
		} else {
			// Already committed.
			return nil, nil
		}
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: *txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, &write, commitTs)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
