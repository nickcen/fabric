/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package stateconcorddb

import (
  proto "github.com/golang/protobuf/proto"
  "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
  "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/stateconcorddb/msgs"
  "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

// encode value encodes the versioned value. starting in v1.3 the encoding begins with a nil
// byte and includes metadata.
func encodeValue(v *statedb.VersionedValue) ([]byte, error) {
  vvMsg := &msgs.VersionedValueProto{
    VersionBytes: v.Version.ToBytes(),
    Value:        v.Value,
    Metadata:     v.Metadata,
  }
  encodedValue, err := proto.Marshal(vvMsg)
  if err != nil {
    return nil, err
  }
  encodedValue = append([]byte{0}, encodedValue...)
  return encodedValue, nil
}

// decodeValue decodes the statedb value bytes using either the old (pre-v1.3) encoding
// or the new (v1.3 and later) encoding that supports metadata.
func decodeValue(encodedValue []byte) (*statedb.VersionedValue, error) {
  msg := &msgs.VersionedValueProto{}
  err := proto.Unmarshal(encodedValue[1:], msg)
  if err != nil {
    return nil, err
  }
  ver, _ := version.NewHeightFromBytes(msg.VersionBytes)
  val := msg.Value
  metadata := msg.Metadata
  // protobuf always makes an empty byte array as nil
  if val == nil {
    val = []byte{}
  }
  return &statedb.VersionedValue{Version: ver, Value: val, Metadata: metadata}, nil
}