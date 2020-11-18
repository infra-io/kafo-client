// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/19 00:10:29

package main

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/avino-plan/kafo-client"
)

const (
	// keySize is the key size for testing.
	keySize = 10

	// concurrency is the number of goroutines for testing.
	concurrency = 1000
)

// testTask is a wrapper wraps task to testTask.
func testTask(task func(no int)) string {
	beginTime := time.Now()
	wg := &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(goId int) {
			defer wg.Done()
			start := goId * keySize
			end := start + keySize
			for j := start; j < end; j++ {
				task(j)
			}
		}(i)
	}
	wg.Wait()
	return time.Now().Sub(beginTime).String()
}

// go test -v -count=1 performance_test.go -run=^TestTcpClient$
func TestTcpClient(t *testing.T) {

	config := kafo.DefaultConfig()
	client, err := kafo.NewTCPClient([]string{"127.0.0.1:5837"}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	writeTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		err := client.Set(data, []byte(data), 0)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("写入消耗时间为 %s！", writeTime)

	time.Sleep(3 * time.Second)

	readTime := testTask(func(no int) {
		data := strconv.Itoa(no)
		_, err := client.Get(data)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Logf("读取消耗时间为 %s！", readTime)
}
