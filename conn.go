// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:33:58

package kafo

import (
	"github.com/FishGoddess/vex"
)

// connection is a pipeline to server.
type connection struct {

	// client does all commands inside.
	client *vex.Client

	// requestChan is a channel storing requests.
	requestChan chan *request
}

// newConnection returns a new connection and an error if failed.
// Network can be one of ["tcp", "tcp4", "tcp6"].
func newConnection(network string, address string) (*connection, error) {

	client, err := vex.NewClient(network, address)
	if err != nil {
		return nil, err
	}

	instance := &connection{
		client:      client,
		requestChan: make(chan *request, 65536),
	}
	go instance.handleRequests()
	return instance, nil
}

// handleRequests handles all requests in request channel.
func (c *connection) handleRequests() {

	for request := range c.requestChan {
		body, err := c.client.Do(request.command, request.args)
		request.resultChan <- &Response{
			Body: body,
			Err:  err,
		}
	}
}

// do will send a request and put result into a result channel.
func (c *connection) do(req *request) <-chan *Response {
	req.resultChan = make(chan *Response, 1)
	c.requestChan <- req
	return req.resultChan
}

// close will discard all requests pending then close itself.
// Return an error if failed.
func (c *connection) close() error {
	close(c.requestChan)
	return c.client.Close()
}
