package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.conf.DBPath, s.conf.Raft)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standaloneStorageReader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	defer txn.Commit()
	for _, b := range batch {
		var err error
		switch v := b.Data.(type) {
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(v.Cf, v.Key), v.Value)
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(v.Cf, v.Key))
		}
		if errors.Is(err, badger.ErrTxnTooBig) {
			_ = txn.Commit()
			txn = s.db.NewTransaction(true)
		}
	}
	return nil
}

var _ storage.StorageReader = &standaloneStorageReader{}

type standaloneStorageReader struct {
	txn *badger.Txn
}

func (s *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v, err := item.ValueCopy(make([]byte, 0))
	return v, err
}

func (s *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)

}

func (s *standaloneStorageReader) Close() {
	s.txn.Discard()
}
