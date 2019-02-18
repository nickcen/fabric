/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package stateconcorddb

import (
// "bytes"
"strings"
"github.com/hyperledger/fabric/common/flogging"
"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
"github.com/pkg/errors"
"github.com/syndtr/goleveldb/leveldb/iterator"
"github.com/hyperledger/fabric/core/ledger/util/concorddb"

)

var logger = flogging.MustGetLogger("stateconcorddb")

var savePointKey = "last_save_point"
var compositeKeySep = "-"

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
  concordInstance *concorddb.ConcordInstance
  databases     map[string]*VersionedDB
}

const (
  address     = "localhost:50051"
)

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() (*VersionedDBProvider){
  concordInstance := concorddb.CreateConcordInstance(address)

  return &VersionedDBProvider{concordInstance, make(map[string]*VersionedDB)}
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
  return &VersionedDB{provider.concordInstance, dbName}, nil
}

// Close closes the underlying db
func (provider *VersionedDBProvider) Close() {

}


// VersionedDB implements VersionedDB interface
type VersionedDB struct {
  db *concorddb.ConcordInstance
  dbName string
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
// do nothing because shared db is used
  return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {
// do nothing because shared db is used
}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
  return nil
}

// BytesKeySupported implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySupported() bool {
  return false
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
  logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
  compositeKey := constructCompositeKey(vdb.dbName, namespace, key)
  dbVal, err := vdb.db.Get(compositeKey)
  if err != nil {
    return nil, err
  }
  if dbVal == nil {
    return nil, nil
  }
  return decodeValue(dbVal)
}

// GetVersion implements method in VersionedDB interface
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
  versionedValue, err := vdb.GetState(namespace, key)
  if err != nil {
    return nil, err
  }
  if versionedValue == nil {
    return nil, nil
  }
  return versionedValue.Version, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
  vals := make([]*statedb.VersionedValue, len(keys))
  for i, key := range keys {
    val, err := vdb.GetState(namespace, key)
    if err != nil {
      return nil, err
    }
    vals[i] = val
  }
  return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
  // return vdb.db.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)
  return nil, nil
}

const optionLimit = "limit"

// GetStateRangeScanIteratorWithMetadata implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {

//   requestedLimit := int32(0)
// // if metadata is provided, validate and apply options
//   if metadata != nil {
// //validate the metadata
//     err := statedb.ValidateRangeMetadata(metadata)
//     if err != nil {
//       return nil, err
//     }
//     if limitOption, ok := metadata[optionLimit]; ok {
//       requestedLimit = limitOption.(int32)
//     }
//   }

// // Note:  metadata is not used for the goleveldb implementation of the range query
//   compositeStartKey := constructCompositeKey(vdb.dbName, namespace, startKey)
//   compositeEndKey := constructCompositeKey(vdb.dbName, namespace, endKey)

//   _ := vdb.db.GetIterator(compositeStartKey, compositeEndKey)

  // return newKVScanner(namespace, dbItr, requestedLimit), nil
  return nil, nil

}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
  return nil, errors.New("ExecuteQuery not supported for leveldb")
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
  return nil, errors.New("ExecuteQueryWithMetadata not supported for leveldb")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
  namespaces := batch.GetUpdatedNamespaces()
  for _, ns := range namespaces {
    updates := batch.GetUpdates(ns)
    for k, vv := range updates {
      compositeKey := constructCompositeKey(vdb.dbName, ns, k)
      logger.Debugf("Channel [%s]: Applying key(string)=[%s] key(bytes)=[%#v]", vdb.dbName, string(compositeKey), compositeKey)

      if vv.Value == nil {
        vdb.db.Delete(compositeKey)
      } else {
        encodedVal, err := encodeValue(vv)
        if err != nil {
          return err
        }
        vdb.db.Set(compositeKey, encodedVal)
      }
    }
  }
// Record a savepoint at a given height
// If a given height is nil, it denotes that we are committing pvt data of old blocks.
// In this case, we should not store a savepoint for recovery. The lastUpdatedOldBlockList
// in the pvtstore acts as a savepoint for pvt data.
  if height != nil {
    vdb.db.Set(constructSavePointKey(vdb.dbName), height.ToBytes())
  }

  return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
  versionBytes, err := vdb.db.Get(constructSavePointKey(vdb.dbName))
  if err != nil {
    return nil, err
  }
  if versionBytes == nil {
    return nil, nil
  }
  version, _ := version.NewHeightFromBytes(versionBytes)
  return version, nil
}

func constructCompositeKey(database string, ns string, key string) string {
  return database + compositeKeySep + ns + compositeKeySep + key
}

func constructSavePointKey(database string) string {
  return constructCompositeKey(database, "_", savePointKey) 
}

func splitCompositeKey(compositeKey string) (string, string) {
  split := strings.Split(compositeKey, compositeKeySep)
  return split[1], split[2]
}

type kvScanner struct {
  namespace            string
  dbItr                iterator.Iterator
  requestedLimit       int32
  totalRecordsReturned int32
}

func newKVScanner(namespace string, dbItr iterator.Iterator, requestedLimit int32) *kvScanner {
  return &kvScanner{namespace, dbItr, requestedLimit, 0}
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error) {

  // if scanner.requestedLimit > 0 && scanner.totalRecordsReturned >= scanner.requestedLimit {
  //   return nil, nil
  // }

  // if !scanner.dbItr.Next() {
  //   return nil, nil
  // }

  // dbKey := scanner.dbItr.Key()
  // dbVal := scanner.dbItr.Value()
  // dbValCopy := make([]byte, len(dbVal))
  // copy(dbValCopy, dbVal)
  // _, key := splitCompositeKey(dbKey)
  // vv, err := decodeValue(dbValCopy)
  // if err != nil {
  //   return nil, err
  // }

  // scanner.totalRecordsReturned++

  // return &statedb.VersionedKV{
  //   CompositeKey: statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
  // // TODO remove dereferrencing below by changing the type of the field
  // // `VersionedValue` in `statedb.VersionedKV` to a pointer
  //   VersionedValue: *vv}, nil
  return nil, nil
}

func (scanner *kvScanner) Close() {
  scanner.dbItr.Release()
}

func (scanner *kvScanner) GetBookmarkAndClose() string {
    // retval := ""
    // if scanner.dbItr.Next() {
    //   dbKey := scanner.dbItr.Key()
    //   _, key := splitCompositeKey(dbKey)
    //   retval = key
    // }
    // scanner.Close()
    // return retval
  return ""
}
