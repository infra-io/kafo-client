// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/18 23:56:29

package kafo

import (
	"strconv"
	"sync"
	"testing"
)

// go test -v -cover -run=^TestTCPClient$
func TestTCPClient(t *testing.T) {

	config := DefaultConfig()
	client, err := NewTCPClient([]string{"127.0.0.1:5837"}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	wg := &sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			key := "key" + strconv.Itoa(seq)
			err := client.Set(key, []byte(key), 0)
			if err != nil {
				t.Fatal(err)
			}

			value, err := client.Get(key)
			if err != nil || string(value) != key {
				t.Fatalf("value %s is wrong due to error %v", string(value), err)
			}
		}(i)
	}
	wg.Wait()
}
