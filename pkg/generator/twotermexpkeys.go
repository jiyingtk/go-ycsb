// Copyright 2018 PingCAP, Inc.
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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package generator

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/pingcap/go-ycsb/pkg/util"
)

//  const (
// 	 // ParetoTheta is the 'theta' of Generized Pareto Distribution.
// 	 ParetoTheta = float64(0)
// 	 // ParetoK is the 'k' of Generized Pareto Distribution.
// 	 ParetoK = float64(0.923)
// 	 // ParetoSigma is the 'sigma' of Generized Pareto Distribution.
// 	 ParetoSigma = float64(226.409)
//  )

type keyRangeUnit struct {
	keyRangeStart  int64
	keyRangeAccess int64
	keyRangeKeys   int64
}

// TwoTermExpKeys generates integers follow TwoTermExpKeys distribution.
type TwoTermExpKeys struct {
	Number
	keyRangeRandMax int64
	keyRangeSize    int64
	keyRangeNum     int64
	keyRangeSet     []*keyRangeUnit
}

// NewTwoTermExpKeys creates the NewTwoTermExpKeys generator.
func NewTwoTermExpKeys(totalKeys int64, keyRangeNum int64, prefixA float64, prefixB float64, prefixC float64, prefixD float64) *TwoTermExpKeys {
	ttek := &TwoTermExpKeys{}

	var amplify int64
	var keyRangeStart int64
	if keyRangeNum <= 0 {
		ttek.keyRangeNum = 1
	} else {
		ttek.keyRangeNum = keyRangeNum
	}
	ttek.keyRangeSize = totalKeys / ttek.keyRangeNum

	for pfx := ttek.keyRangeNum; pfx >= 1; pfx-- {
		keyRangeP := prefixA*math.Exp(prefixB*float64(pfx)) + prefixC*math.Exp(prefixD*float64(pfx))
		if keyRangeP < math.Pow10(-16) {
			keyRangeP = float64(0)
		}
		if amplify == 0 && keyRangeP > 0 {
			amplify = int64(math.Floor(1/keyRangeP)) + 1
		}

		pUnit := &keyRangeUnit{}
		pUnit.keyRangeStart = keyRangeStart
		if 0.0 >= keyRangeP {
			pUnit.keyRangeAccess = 0
		} else {
			pUnit.keyRangeAccess = int64(math.Floor(float64(amplify) * keyRangeP))
		}
		pUnit.keyRangeKeys = ttek.keyRangeSize
		ttek.keyRangeSet = append(ttek.keyRangeSet, pUnit)
		keyRangeStart += pUnit.keyRangeAccess

		fmt.Println("key range ", pfx, " access weight : ", pUnit.keyRangeAccess)
	}
	ttek.keyRangeRandMax = keyRangeStart
	fmt.Println("total access weight", keyRangeStart)

	randLocal := rand.New(rand.NewSource(ttek.keyRangeRandMax))
	for i := int64(0); i < keyRangeNum; i++ {
		pos := randLocal.Int63n(keyRangeNum)

		tmp := ttek.keyRangeSet[i]
		ttek.keyRangeSet[i] = ttek.keyRangeSet[pos]
		ttek.keyRangeSet[pos] = tmp
	}

	offset := int64(0)
	for _, pUnit := range ttek.keyRangeSet {
		pUnit.keyRangeStart = offset
		offset += pUnit.keyRangeAccess
	}

	return ttek
}

// Next implements the Generator Next interface.
func (t *TwoTermExpKeys) Next(r *rand.Rand) int64 {
	return 0
}

// DistGetKeyID implements DistGetKeyID.
func (t *TwoTermExpKeys) DistGetKeyID(initRand int64, keyDistA float64, keyDistB float64) int64 {
	keyRangeRand := initRand % t.keyRangeRandMax

	start := 0
	end := len(t.keyRangeSet)
	for start+1 < end {
		mid := start + (end-start)/2
		if keyRangeRand < t.keyRangeSet[mid].keyRangeStart {
			end = mid
		} else {
			start = mid
		}
	}
	keyRangeID := start

	var keyOffset, keySeed int64
	if keyDistA == 0.0 || keyDistB == 0.0 {
		keyOffset = initRand % t.keyRangeSize
	} else {
		u := float64(initRand%t.keyRangeSize) / float64(t.keyRangeSize)
		keySeed = int64(math.Ceil(math.Pow(u/keyDistA, 1/keyDistB)))
		// randKey := rand.New(rand.NewSource(keySeed))
		// keyOffset = randKey.Int63n(t.keyRangeSize)
		keyOffset = util.Hash64(keySeed) % t.keyRangeSize
	}
	return t.keyRangeSize*int64(keyRangeID) + keyOffset
}
