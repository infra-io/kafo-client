// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:33:01

package kafo

import "encoding/json"

// Status 是缓存状态结构体。
type Status struct {

	// Count 是键值对的数量。
	Count int `json:"count"`

	// KeySize 是 key 占用的大小
	KeySize int64 `json:"keySize"`

	// ValueSize 是 value 占用的大小。
	ValueSize int64 `json:"valueSize"`
}

// request 是请求结构体。
type request struct {

	// command 是执行的命令。
	command byte

	// args 是执行的参数。
	args [][]byte

	// resultChan 是用于接收结果的管道。
	resultChan chan *Response
}

// Response 是响应结构体。
type Response struct {

	// Body 是响应体。
	Body []byte

	// Err 是响应的错误。
	Err error
}

// wrapErrorToResponseChan wraps an error to a channel of response.
func wrapErrorToResponseChan(err error) <-chan *Response {
	responseChan := make(chan *Response, 1)
	responseChan <- &Response{
		Err: err,
	}
	return responseChan
}

// toStatus parses response and returns a status instance of it.
// Return an error if failed.
func (r *Response) toStatus() (*Status, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	status := &Status{}
	return status, json.Unmarshal(r.Body, status)
}

// toNodes parses response and returns nodes slice of it.
// Return an error if failed.
func (r *Response) toNodes() ([]string, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	var nodes []string
	return nodes, json.Unmarshal(r.Body, &nodes)
}
