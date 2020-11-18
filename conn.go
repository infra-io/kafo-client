// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:33:58

package kafo

import (
	"sync"

	"github.com/FishGoddess/vex"
)

// connection is a pipeline to server.
type connection struct {

	// config stores all configurations of connections.
	config *Config

	// client does all commands inside.
	client *vex.Client

	// lock is for safe-concurrency.
	lock *sync.Mutex
}

// newConnection returns a new connection and an error if failed.
// Network can be one of ["tcp", "tcp4", "tcp6"].
func newConnection(address string, config *Config) (*connection, error) {

	client, err := vex.NewClient(config.Network, address)
	if err != nil {
		return nil, err
	}
	return &connection{
		client: client,
		config: config,
		lock:   &sync.Mutex{},
	}, nil
}

// do will execute command with args.
func (c *connection) do(command byte, args [][]byte) (body []byte, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.client.Do(command, args)
}

// close will discard all requests pending then close itself.
// Return an error if failed.
func (c *connection) close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.client.Close()
}
