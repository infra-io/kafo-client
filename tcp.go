// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:34:49

package kafo

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"stathat.com/c/consistent"
)

const (
	// getCommand is the command of get operation.
	getCommand = byte(1)

	// setCommand is the command of set operation.
	setCommand = byte(2)

	// deleteCommand is the command of delete operation.
	deleteCommand = byte(3)

	// statusCommand is the command of status operation.
	statusCommand = byte(4)

	// nodesCommand is the command of nodes operation.
	nodesCommand = byte(5)
)

// TCPClient is the client of tcp Network.
type TCPClient struct {

	// config stores all configurations of connections.
	config *Config

	// data stores all connections.
	data map[string]*connection

	// deadlines stores all deadline of connections.
	deadlines map[string]time.Time

	// circle stores the relation of data and node.
	circle *consistent.Consistent

	// lock is for safe-concurrency.
	lock *sync.Mutex
}

// NewTCPClient returns a new tcp client with given config and an error if failed.
func NewTCPClient(addresses []string, config Config) (*TCPClient, error) {

	client := &TCPClient{
		config:    &config,
		data:      map[string]*connection{},
		deadlines: map[string]time.Time{},
		lock:      &sync.Mutex{},
	}

	errorCount := 0
	for _, address := range addresses {
		err := client.addConnection(address)
		if err != nil {
			errorCount++
		}
	}

	if errorCount == len(addresses) {
		return client, fmt.Errorf("failed after trying to add %d connections", errorCount)
	}

	client.circle = consistent.New()
	client.circle.NumberOfReplicas = config.NumberOfReplicas
	client.updateCircle()

	go client.autoGc()
	go client.autoUpdateCircle()
	return client, nil
}

// autoGc clears all dead connections at fixed duration.
func (tc *TCPClient) autoGc() {

	ticker := time.NewTicker(tc.config.GcDuration)
	for {
		select {
		case <-ticker.C:
			tc.lock.Lock()
			for address, conn := range tc.data {
				if deadline, ok := tc.deadlines[address]; !ok || deadline.Before(time.Now()) {
					conn.close()
					delete(tc.data, address)
					delete(tc.deadlines, address)
				}
			}
			tc.lock.Unlock()
		}
	}
}

// getConnection returns connection of address or not.
func (tc *TCPClient) getConnection(address string) (*connection, bool) {

	conn, ok := tc.data[address]
	if !ok {
		return nil, false
	}

	if deadline, ok := tc.deadlines[address]; !ok || deadline.Before(time.Now()) {
		conn.close()
		delete(tc.data, address)
		delete(tc.deadlines, address)
		return nil, false
	}
	return conn, true
}

// addConnection adds a new connection of address to tc.
func (tc *TCPClient) addConnection(address string) error {

	conn, err := newConnection(tc.config.Network, address)
	if err != nil {
		return err
	}
	tc.data[address] = conn
	tc.deadlines[address] = time.Now().Add(tc.config.Ttl)
	return nil
}

func (tc *TCPClient) doOnAddress(address string, task func(conn *connection) <-chan *Response) <-chan *Response {

	var err error
	for i := 0; i < tc.config.MaxTimesOfGetConnection; i++ {
		if conn, ok := tc.getConnection(address); ok {
			return task(conn)
		}
		err = tc.addConnection(address)
	}
	return wrapErrorToResponseChan(fmt.Errorf("failed to get connection after trying %d times due to %v",
		tc.config.MaxTimesOfGetConnection, err))
}

// nodes returns nodes of cluster and an error if failed.
func (tc *TCPClient) nodes() ([]string, error) {

	var err error
	addresses := tc.circle.Members()
	for _, address := range addresses {
		responseChan := tc.doOnAddress(address, func(conn *connection) <-chan *Response {
			return conn.do(&request{
				command:    nodesCommand,
				args:       nil,
				resultChan: make(chan *Response, 1),
			})
		})
		response := <-responseChan
		if nodes, err := response.toNodes(); err == nil {
			return nodes, nil
		}
	}
	return nil, err
}

// updateCircle will update hash circle to cluster.
func (tc *TCPClient) updateCircle() {

	for i := 0; i < tc.config.MaxTimesOfUpdateCircle; i++ {
		nodes, err := tc.nodes()
		if err == nil {
			tc.circle.Set(nodes)
		}
	}
}

// autoUpdateCircle updates circle at fixed duration.
func (tc *TCPClient) autoUpdateCircle() {

	ticker := time.NewTicker(tc.config.UpdateCircleDuration)
	for {
		select {
		case <-ticker.C:
			tc.lock.Lock()
			tc.updateCircle()
			tc.lock.Unlock()
		}
	}
}

// Get returns the value of key and an error if failed.
func (tc *TCPClient) Get(key string) <-chan *Response {

	address, err := tc.circle.Get(key)
	if err != nil {
		return wrapErrorToResponseChan(err)
	}

	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.doOnAddress(address, func(conn *connection) <-chan *Response {
		return conn.do(&request{
			command:    getCommand,
			args:       [][]byte{[]byte(key)},
			resultChan: make(chan *Response, 1),
		})
	})
}

// Set adds the key and value with given ttl to cache.
// Returns an error if failed.
func (tc *TCPClient) Set(key string, value []byte, ttl int64) <-chan *Response {

	address, err := tc.circle.Get(key)
	if err != nil {
		return wrapErrorToResponseChan(err)
	}

	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.doOnAddress(address, func(conn *connection) <-chan *Response {
		ttlBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(ttlBytes, uint64(ttl))
		return conn.do(&request{
			command:    setCommand,
			args:       [][]byte{ttlBytes, []byte(key), value},
			resultChan: make(chan *Response, 1),
		})
	})
}

// Delete deletes the value of key and returns an error if failed.
func (tc *TCPClient) Delete(key string) <-chan *Response {

	address, err := tc.circle.Get(key)
	if err != nil {
		return wrapErrorToResponseChan(err)
	}

	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.doOnAddress(address, func(conn *connection) <-chan *Response {
		return conn.do(&request{
			command:    deleteCommand,
			args:       [][]byte{[]byte(key)},
			resultChan: make(chan *Response, 1),
		})
	})
}

// Status returns the status of cache and an error if failed.
func (tc *TCPClient) Status() (*Status, error) {

	tc.lock.Lock()
	defer tc.lock.Unlock()

	// Fetch from all nodes
	addresses := tc.circle.Members()
	responseChannels := make([]<-chan *Response, 0, len(addresses))
	for _, address := range addresses {
		responseChannels = append(responseChannels, tc.doOnAddress(address, func(conn *connection) <-chan *Response {
			return conn.do(&request{
				command:    statusCommand,
				args:       nil,
				resultChan: make(chan *Response, 1),
			})
		}))
	}

	// Summary
	result := &Status{}
	for _, responseChan := range responseChannels {
		response := <-responseChan
		status, err := response.toStatus()
		if err != nil {
			return nil, err
		}
		result.Count += status.Count
		result.KeySize += status.KeySize
		result.ValueSize += status.ValueSize
	}
	return result, nil
}

// Nodes returns the nodes of cluster and an error if failed.
func (tc *TCPClient) Nodes() ([]string, error) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.nodes()
}

// Close closes all connections.
func (tc *TCPClient) Close() {

	tc.lock.Lock()
	defer tc.lock.Unlock()

	for _, conn := range tc.data {
		conn.close()
	}

	for address, _ := range tc.deadlines {
		delete(tc.deadlines, address)
	}
}
