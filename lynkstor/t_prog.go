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

func (cn *Connector) ProgNew(key skv.ProgKey, val skv.ValueObject, opts *skv.ProgWriteOptions) skv.Result {

	if opts == nil {
		opts = &skv.ProgWriteOptions{}
	}

	opts.Actions = opts.Actions | skv.ProgOpCreate

	return cn.ProgPut(key, val, opts)
}

func (cn *Connector) ProgPut(key skv.ProgKey, val skv.ValueObject, opts *skv.ProgWriteOptions) skv.Result {
	if !key.Valid() || !val.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	pc := &skv.ProgKeyValueCommit{
		Key:     &key,
		Value:   val.Value,
		Options: opts,
	}
	bs, err := proto.Marshal(pc)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("progput", bs)
}

func (cn *Connector) ProgGet(key skv.ProgKey) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("progget", bs)
}

func (cn *Connector) ProgDel(key skv.ProgKey, opts *skv.ProgWriteOptions) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	pc := &skv.ProgKeyValueCommit{
		Key:     &key,
		Options: opts,
	}
	bs, err := proto.Marshal(pc)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("progdel", bs)
}

func (cn *Connector) ProgScan(offset, cutset skv.ProgKey, limit int) skv.Result {
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
	return cn.Cmd("progscan", k1, k2, limit)
}

func (cn *Connector) ProgRevScan(offset, cutset skv.ProgKey, limit int) skv.Result {
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
	return cn.Cmd("progrevscan", k1, k2, limit)
}

func (cn *Connector) ProgIncr(key skv.ProgKey, incr int64) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("progincr", bs, incr)
}

func (cn *Connector) ProgMeta(key skv.ProgKey) skv.Result {
	if !key.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&key)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("progmeta", bs)
}
