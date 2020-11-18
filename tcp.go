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
	"encoding/json"
	"fmt"
	"strings"
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

	// redirectPrefix is the prefix of redirect error.
	redirectPrefix = "redirect to node "
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
	client.circle.Set(addresses)
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

	conn, err := newConnection(address, tc.config)
	if err != nil {
		return err
	}
	tc.data[address] = conn
	tc.deadlines[address] = time.Now().Add(tc.config.Ttl)
	return nil
}

// nodes returns nodes of cluster and an error if failed.
func (tc *TCPClient) nodes() ([]string, error) {

	addresses := tc.circle.Members()
	for _, address := range addresses {

		conn, ok := tc.getConnection(address)
		if !ok {
			continue
		}

		body, err := conn.do(nodesCommand, nil)
		if err != nil {
			continue
		}
		var nodes []string
		return nodes, json.Unmarshal(body, &nodes)
	}
	return nil, fmt.Errorf("failed to get nodes after connecting %d addresses", len(addresses))
}

// updateCircle will update hash circle to cluster.
func (tc *TCPClient) updateCircle() {

	for i := 0; i < tc.config.MaxRetryTimes; i++ {
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

// do will use key to get a address and try to connect it.
// If connection is ok, then command and args will be sent through this connection.
func (tc *TCPClient) do(key string, command byte, args [][]byte) (body []byte, err error) {

	// Guess why we do this? Because we think the first time adding connection is not counted as retry
	maxRetryTimes := 3
	if tc.config.MaxRetryTimes > 3 {
		maxRetryTimes = tc.config.MaxRetryTimes
	}

	var address string
	for i := 0; i < maxRetryTimes; i++ {

		// Select address of key
		address, err = tc.circle.Get(key)
		if err != nil {
			continue
		}

		// Get connection of address and add a new one if not found
		conn, ok := tc.getConnection(address)
		if !ok {
			err = tc.addConnection(address)
			continue
		}

		// Use connection to do something
		body, err = conn.do(command, args)
		if err != nil {
			errMsg := err.Error()
			if strings.HasPrefix(errMsg, redirectPrefix) {
				address = strings.TrimPrefix(err.Error(), redirectPrefix)
				continue
			}

			// An existing connection was forcibly closed by the remote host
			if strings.Contains(errMsg, "closed by the remote host") {
				if nodes, err := tc.nodes(); err == nil {
					tc.circle.Set(nodes)
					continue
				}
			}
		}
		return body, nil
	}
	return nil, fmt.Errorf("failed after trying %d times due to %v", maxRetryTimes, err)
}

// Get returns the value of key and an error if failed.
func (tc *TCPClient) Get(key string) ([]byte, error) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	return tc.do(key, getCommand, [][]byte{[]byte(key)})
}

// Set adds the key and value with given ttl to cache.
// Returns an error if failed.
func (tc *TCPClient) Set(key string, value []byte, ttl int64) error {

	tc.lock.Lock()
	defer tc.lock.Unlock()

	ttlBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ttlBytes, uint64(ttl))
	_, err := tc.do(key, setCommand, [][]byte{ttlBytes, []byte(key), value})
	return err
}

// Delete deletes the value of key and returns an error if failed.
func (tc *TCPClient) Delete(key string) error {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	_, err := tc.do(key, deleteCommand, [][]byte{[]byte(key)})
	return err
}

// Status returns the status of kafo and an error if failed.
func (tc *TCPClient) Status() (*Status, error) {

	tc.lock.Lock()
	defer tc.lock.Unlock()

	// Fetch from all nodes
	result := &Status{}
	addresses := tc.circle.Members()
	for _, address := range addresses {

		conn, ok := tc.getConnection(address)
		if !ok {
			if err := tc.addConnection(address); err != nil {
				continue
			}
			if conn, ok = tc.getConnection(address); !ok {
				continue
			}
		}

		body, err := conn.do(statusCommand, nil)
		status := &Status{}
		err = json.Unmarshal(body, status)
		if err != nil {
			continue
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
