// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// callback函数的参数就是我们想要的那个值, 我们只用取到这个值就好了
// callback(bc.Regions.pendingPeers[storeID])
var cbValue core.RegionsContainer
func callBack (regiomContainer core.RegionsContainer) {
	cbValue = regiomContainer
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. select all suitable stores
	var suitableStores []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		// should be up and the down time cannot be longer than MaxStoreDownTime
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	// fix bug: 由于是move操作, 所以至少得有两个store
	if len(suitableStores) < 2 {
		return nil
	}
	// 2. sort them according to their region size.
	sort.Sort(core.StoreInfoSlice(suitableStores))
	// 3. then tries to find regions to move from the store with the biggest region size.
	var region *core.RegionInfo
	var index int
	for i, store := range suitableStores {
		cluster.GetPendingRegionsWithLock(store.GetID(), callBack)
		if region = cbValue.RandomRegion([]byte{}, []byte{}); region != nil {
			index = i
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), callBack)
		if region = cbValue.RandomRegion([]byte{}, []byte{}); region != nil {
			index = i
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), callBack)
		if region = cbValue.RandomRegion([]byte{}, []byte{}); region != nil {
			index = i
			break
		}
	}
	// 4. Finally it will select out the region to move
	if region == nil {
		return nil
	}
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	// 5. select a store as the target. Actually, select the store with the smallest region size.
	var targetStore *core.StoreInfo
	storeIds := region.GetStoreIds()
	for i := len(suitableStores)-1; i >= 0; i-- {
		if i <= index {
			return nil
		}
		// 找到没有存这个region的store
		if _, ok := storeIds[suitableStores[i].GetID()]; !ok {
			targetStore = suitableStores[i]
			break
		}
	}
	// 6. judge whether this movement is valuable
	//    by checking the difference between region sizes of the original store and the target store
	//    How much? the answer is bigger than two times of the approximate size of the region
	if suitableStores[index].GetRegionSize() - targetStore.GetRegionSize() < region.GetApproximateSize()<<1 {
		return nil
	}
	// 7. Finally, allocate a new peer on the target store and create a move peer operator.
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}
	operator, err := operator.CreateMovePeerOperator("my move" ,cluster, region, operator.OpBalance,
		suitableStores[index].GetID(), targetStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return operator
}
