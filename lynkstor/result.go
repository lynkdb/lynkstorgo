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
	"encoding/json"
	"errors"

	"github.com/lessos/lessgo/types"
	"github.com/lynkdb/iomix/skv"
)

const (
	kvobj_t_v1     uint8 = 0x01
	value_ns_bytes uint8 = 0x00
	value_ns_json  uint8 = 20
)

type Result struct {
	status uint8
	data   []byte
	cap    int
	items  []*Result
}

func newResult(status uint8, err error) *Result {

	rs := &Result{
		status: status,
	}

	if err != nil {
		if status == 0 {
			rs.status = skv.ResultError
		}
		rs.data = []byte(err.Error())
	}

	return rs
}

func (rs *Result) Status() uint8 {
	return rs.status
}

func (rs *Result) OK() bool {
	return rs.status == skv.ResultOK
}

func (rs *Result) NotFound() bool {
	return rs.status == skv.ResultNotFound
}

func (rs *Result) Bytes() []byte {
	if len(rs.data) > 1 {
		if rs.data[0] == kvobj_t_v1 {
			offset := int(rs.data[1]) + 2
			if offset < len(rs.data) {
				return rs.data[offset:]
			}
		}
	}
	return rs.data
}

func (rs *Result) Bytex() types.Bytex {
	return skv.ValueBytes(rs.Bytes()).Bytex()
}

func (rs *Result) String() string {
	return skv.ValueBytes(rs.Bytes()).String()
}

func (rs *Result) Bool() bool {
	return skv.ValueBytes(rs.Bytes()).Bool()
}

func (rs *Result) Int() int {
	return int(rs.Int64())
}

func (rs *Result) Int8() int8 {
	return int8(rs.Int64())
}

func (rs *Result) Int16() int16 {
	return int16(rs.Int64())
}

func (rs *Result) Int32() int32 {
	return int32(rs.Int64())
}

func (rs *Result) Int64() int64 {
	return skv.ValueBytes(rs.Bytes()).Int64()
}

func (rs *Result) Uint() uint {
	return uint(rs.Uint64())
}

func (rs *Result) Uint8() uint8 {
	return uint8(rs.Uint64())
}

func (rs *Result) Uint16() uint16 {
	return uint16(rs.Uint64())
}

func (rs *Result) Uint32() uint32 {
	return uint32(rs.Uint64())
}

func (rs *Result) Uint64() uint64 {
	return skv.ValueBytes(rs.Bytes()).Uint64()
}

func (rs *Result) Float32() float32 {
	return float32(rs.Float64())
}

func (rs *Result) Float64() float64 {
	return skv.ValueBytes(rs.Bytes()).Float64()
}

func (rs *Result) ListLen() int {
	return len(rs.items)
}

func (rs *Result) List() []skv.Result {
	ls := []skv.Result{}
	for _, v := range rs.items {
		ls = append(ls, v)
	}
	return ls
}

func (rs *Result) KvLen() int {
	return len(rs.items) / 2
}

func (rs *Result) KvEach(fn func(entry *skv.ResultEntry) int) int {
	for i := 1; i < len(rs.items); i += 2 {
		if fn(&skv.ResultEntry{
			Key:   rs.items[i-1].data,
			Value: rs.items[i].data,
		}) != 0 {
			return (i + 1) / 2
		}
	}
	return rs.KvLen()
}

func (rs *Result) KvEntry(i int) *skv.ResultEntry {
	if i < 0 {
		i = 0
	} else {
		i = i * 2
	}
	if i+1 < len(rs.items) {
		return &skv.ResultEntry{
			Key:   rs.items[i].data,
			Value: rs.items[i+1].data,
		}
	}
	return nil
}

func (rs *Result) KvList() []*skv.ResultEntry {
	ls := []*skv.ResultEntry{}
	for i := 1; i < len(rs.items); i += 2 {
		ls = append(ls, &skv.ResultEntry{
			Key:   rs.items[i-1].data,
			Value: rs.items[i].data,
		})
	}
	return ls
}

func (rs *Result) Decode(obj interface{}) error {
	bs := rs.Bytes()
	if len(bs) < 3 {
		return errors.New("json: invalid format")
	}
	return json.Unmarshal(bs[1:], obj)
}

func (rs *Result) Meta() *skv.MetaObject {
	if len(rs.data) > 1 {
		if rs.data[0] == kvobj_t_v1 {
			offset := int(rs.data[1]) + 2
			if offset <= len(rs.data) {
				return skv.MetaObjectDecode(rs.data[2:offset])
			}
		}
	}
	return nil
}
