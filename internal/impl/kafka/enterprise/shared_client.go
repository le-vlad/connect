// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"errors"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	errSharedClientNameDuplicate = errors.New("a duplicate name for shared clients has been detected")
	errSharedClientNameNotFound  = errors.New("shared client not found")
)

func setSharedClient(name string, client *kgo.Client, res *service.Resources) error {
	reg := getSharedClientRegister(res)
	return reg.set(name, client)
}

func popSharedClient(name string, res *service.Resources) (*kgo.Client, error) {
	reg := getSharedClientRegister(res)
	return reg.pop(name)
}

func useSharedClient(name string, res *service.Resources, fn func(*kgo.Client) error) error {
	reg := getSharedClientRegister(res)
	return reg.use(name, fn)
}

type sharedClientRegister struct {
	mut     sync.RWMutex
	clients map[string]*kgo.Client
}

func (r *sharedClientRegister) set(name string, client *kgo.Client) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	if r.clients == nil {
		r.clients = map[string]*kgo.Client{}
	}

	_, exists := r.clients[name]
	if exists {
		return errSharedClientNameDuplicate
	}

	r.clients[name] = client
	return nil
}

func (r *sharedClientRegister) pop(name string) (*kgo.Client, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	if r.clients == nil {
		return nil, errSharedClientNameNotFound
	}

	e, exists := r.clients[name]
	if !exists {
		return nil, errSharedClientNameNotFound
	}

	delete(r.clients, name)
	return e, nil
}

func (r *sharedClientRegister) use(name string, fn func(*kgo.Client) error) error {
	r.mut.RLock()
	defer r.mut.RUnlock()

	if r.clients == nil {
		return errSharedClientNameNotFound
	}

	e, exists := r.clients[name]
	if !exists {
		return errSharedClientNameNotFound
	}

	return fn(e)
}

//------------------------------------------------------------------------------

type sharedClientKeyType int

var sharedClientKey sharedClientKeyType

func getSharedClientRegister(res *service.Resources) *sharedClientRegister {
	reg, _ := res.GetOrSetGeneric(sharedClientKey, &sharedClientRegister{})
	return reg.(*sharedClientRegister)
}
