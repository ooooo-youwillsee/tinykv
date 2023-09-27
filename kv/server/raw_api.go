package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{
		Data: storage.Put{
			Cf:    req.GetCf(),
			Key:   req.GetKey(),
			Value: req.GetValue(),
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{
		Data: storage.Delete{
			Cf:  req.GetCf(),
			Key: req.GetKey(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	var kvs []*kvrpcpb.KvPair
	it := reader.IterCF(req.GetCf())
	var cnt uint32 = 0
	for it.Seek(req.GetStartKey()); it.Valid(); it.Next() {
		item := it.Item()
		value, _ := item.ValueCopy(make([]byte, 0))
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(make([]byte, 0)),
			Value: value,
		})
		cnt += 1
		if cnt == req.GetLimit() {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
