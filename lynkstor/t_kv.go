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
	"strconv"

	"github.com/lynkdb/iomix/skv"
)

func (c *Connector) KvNew(key []byte, value interface{}, opts *skv.KvWriteOptions) skv.Result {
	args := []interface{}{
		key, value_encode(value), "NX",
	}
	if opts != nil && opts.Ttl > 0 {
		args = append(args, "PX")
		args = append(args, strconv.FormatInt(opts.Ttl, 10))
	}
	return c.Cmd("kvnew", args...)
}

func (c *Connector) KvPut(key []byte, value interface{}, opts *skv.KvWriteOptions) skv.Result {
	args := []interface{}{
		key, value_encode(value),
	}
	if opts != nil && opts.Ttl > 0 {
		args = append(args, "PX")
		args = append(args, strconv.FormatInt(opts.Ttl, 10))
	}
	return c.Cmd("kvput", args...)
}

func (c *Connector) KvGet(key []byte) skv.Result {
	return c.Cmd("kvget", key)
}

func (c *Connector) KvDel(keys ...[]byte) skv.Result {
	args := []interface{}{}
	for _, v := range keys {
		args = append(args, v)
	}
	return c.Cmd("kvdel", args...)
}

func (c *Connector) KvScan(offset, cutset []byte, limit int) skv.Result {
	return c.Cmd("kvscan", offset, cutset, limit)
}

func (c *Connector) KvRevScan(offset, cutset []byte, limit int) skv.Result {
	return c.Cmd("kvrevscan", offset, cutset, limit)
}

func (c *Connector) KvIncr(key []byte, increment int64) skv.Result {
	return c.Cmd("kvincr", key, increment)
}

func (c *Connector) KvMeta(key []byte) skv.Result {
	return c.Cmd("kvmeta", key)
}

// func (c *Connector) KvBatch(batch *skv.KvEngineBatch, opts *skv.KvWriteOptions) error {
// 	return nil
// }
