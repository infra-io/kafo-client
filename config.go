// Copyright 2020 Ye Zi Jie.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.
//
// Author: FishGoddess
// Email: fishgoddess@qq.com
// Created at 2020/11/17 22:26:23

package kafo

import "time"

// Config is the type of config.
type Config struct {

	// Network is the type of Network, which can be one of ["tcp", "tcp4", "tcp6"].
	Network string

	// Ttl is the life of connection.
	Ttl time.Duration

	// GcDuration is the duration between two gc operations.
	GcDuration time.Duration

	// NumberOfReplicas is the number of hash replicas.
	// Notice that it should equals to server.
	NumberOfReplicas int

	// MaxRetryTimes is the max retry times when something wrong happens.
	MaxRetryTimes int

	// UpdateCircleDuration is the duration between two circle updating operations.
	UpdateCircleDuration time.Duration
}

// DefaultConfig returns a default config.
func DefaultConfig() Config {
	return Config{
		Network:              "tcp",
		Ttl:                  10 * time.Minute,
		GcDuration:           15 * time.Minute,
		NumberOfReplicas:     1024,
		MaxRetryTimes:        5,
		UpdateCircleDuration: time.Minute,
	}
}
