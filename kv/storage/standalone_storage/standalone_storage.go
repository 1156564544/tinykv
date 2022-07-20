package standalone_storage

import (
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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	newstorage := new(StandAloneStorage)
	newstorage.db = engine_util.CreateDB(conf.DBPath, conf.Raft)
	return newstorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() (err error) {
	// Your Code Here (1).
	err = s.db.Close()
	return
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := &StorageAloneReader{txn}
	return reader, nil
}

type StorageAloneReader struct {
	txn *badger.Txn
}

func (reader *StorageAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		return []byte(nil), nil
	} else {
		return val, nil
	}
}

func (reader *StorageAloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, reader.txn)
	return iter
}

func (reader *StorageAloneReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for i := 0; i < len(batch); i++ {
		modify := batch[i]
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
