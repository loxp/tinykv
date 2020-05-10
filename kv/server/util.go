package server

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func iterNextKeys(iter engine_util.DBIterator, startKey []byte, limit uint32) ([]*kvrpcpb.KvPair, error) {
	var result []*kvrpcpb.KvPair

	iter.Seek(startKey)
	for iter.Valid() {
		kvPair := &kvrpcpb.KvPair{}
		item := iter.Item()
		kvPair.Key = item.KeyCopy(nil)
		valueCopy, err := item.ValueCopy(kvPair.Value)
		if err != nil {
			return nil, err
		}
		kvPair.Value = valueCopy
		result = append(result, kvPair)
		if len(result) == int(limit) {
			break
		}

		iter.Next()
	}

	return result, nil
}
