package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	response := &kvrpcpb.RawGetResponse{}
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	key := req.Key
	cf := req.Cf
	value, err := reader.GetCF(cf, key)
	if value == nil {
		response.Error = fmt.Sprintf("key not found")
		response.NotFound = true
		return response, nil
	}
	response.Value = value
	response.NotFound = false
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	value := req.Value
	key := req.Key
	cf := req.Cf
	put := storage.Put{key, value, cf}
	modify := storage.Modify{put}
	batch := make([]storage.Modify, 1)
	batch[0] = modify
	err := server.storage.Write(req.Context, batch)
	res := new(kvrpcpb.RawPutResponse)
	if err != nil {
		res.Error = err.Error()
		return res, nil
	}
	res.RegionError = nil
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.Key
	cf := req.Cf
	del := storage.Delete{key, cf}
	modify := storage.Modify{del}
	batch := make([]storage.Modify, 1)
	batch[0] = modify
	err := server.storage.Write(req.Context, batch)
	res := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		res.Error = err.Error()
		return res, nil
	}
	res.RegionError = nil
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	res := new(kvrpcpb.RawScanResponse)
	if err != nil {
		res.Error = err.Error()
		res.Kvs = nil
		return res, nil
	}
	iter := reader.IterCF(req.Cf)
	kvs := make([]*kvrpcpb.KvPair, 0)
	start := req.GetStartKey()
	limit := req.GetLimit()
	iter.Seek(start)
	for iter.Valid() && limit > 0 {
		kvpair := new(kvrpcpb.KvPair)
		dbitem := iter.Item()
		kvpair.Key = dbitem.Key()
		kvpair.Value, _ = dbitem.Value()
		kvs = append(kvs, kvpair)
		iter.Next()
		limit -= 1
	}
	res.Kvs = kvs
	return res, nil
}
