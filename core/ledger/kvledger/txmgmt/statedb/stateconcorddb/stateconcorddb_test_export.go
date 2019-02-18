/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stateconcorddb

import (
  "testing"

  "github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

// TestVDBEnv provides a level db backed versioned db for testing
type TestVDBEnv struct {
  t          testing.TB
  DBProvider statedb.VersionedDBProvider
}

// NewTestVDBEnv instantiates and new concord db backed TestVDB
func NewTestVDBEnv(t testing.TB) *TestVDBEnv {
  t.Logf("Creating new TestVDBEnv")
  dbProvider := NewVersionedDBProvider()
  return &TestVDBEnv{t, dbProvider}
}

// Cleanup closes the db
func (env *TestVDBEnv) Cleanup() {
  env.t.Logf("Cleaningup TestVDBEnv")

  concorddbProvider, _ := env.DBProvider.(*VersionedDBProvider)
  concorddbProvider.concordInstance.DropDatabase()

  env.DBProvider.Close()
}