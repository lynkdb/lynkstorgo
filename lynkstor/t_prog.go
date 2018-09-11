// Copyright 2018 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lynkstor

import (
	"github.com/golang/protobuf/proto"
	"github.com/lynkdb/iomix/skv"
)

func (cn *Connector) KvProgNew(key skv.KvProgKey, val skv.KvEntry, opts *skv.KvProgWriteOptions) skv.Result {

	if opts == nil {
		opts = &skv.KvProgWriteOptions{}
	}

	opts.Actions = opts.Actions | skv.KvProgOpCreate

	return cn.KvProgPut(key, val, opts)
}

func (cn *Connector) KvProgPut(key skv.KvProgKey, val skv.KvEntry, opts *skv.KvProgWriteOptions) skv.Result {
	if !key.Valid() || !val.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	pc := &skv.KvProgKeyValueCommit{
		Key:     &key,
		Value:   val.Value,
		Options: opts,
	}
	bs, err := proto.Marshal(pc)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogput", bs)
}

func (cn *Connector) KvProgGet(key skv.KvProgKey) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogget", bs)
}

func (cn *Connector) KvProgDel(key skv.KvProgKey, opts *skv.KvProgWriteOptions) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	pc := &skv.KvProgKeyValueCommit{
		Key:     &key,
		Options: opts,
	}
	bs, err := proto.Marshal(pc)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogdel", bs)
}

func (cn *Connector) KvProgScan(offset, cutset skv.KvProgKey, limit int) skv.Result {
	if !offset.Valid() || !cutset.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	k1, err := proto.Marshal(&offset)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	k2, err := proto.Marshal(&cutset)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogscan", k1, k2, limit)
}

func (cn *Connector) KvProgRevScan(offset, cutset skv.KvProgKey, limit int) skv.Result {
	if !offset.Valid() || !cutset.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	k1, err := proto.Marshal(&offset)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	k2, err := proto.Marshal(&cutset)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogrevscan", k1, k2, limit)
}

func (cn *Connector) KvProgIncr(key skv.KvProgKey, incr int64) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogincr", bs, incr)
}

func (cn *Connector) KvProgMeta(key skv.KvProgKey) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("kvprogmeta", bs)
}
