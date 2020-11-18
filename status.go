// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:33:01

package kafo

// Status is the status of kafo.
type Status struct {

	// Count records how many entries storing in kafo.
	Count int `json:"count"`

	// KeySize records the size of keys.
	KeySize int64 `json:"keySize"`

	// ValueSize records the size of value.
	ValueSize int64 `json:"valueSize"`
}
