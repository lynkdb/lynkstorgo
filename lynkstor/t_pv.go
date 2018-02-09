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
	"path/filepath"
	"strings"

	"github.com/lynkdb/iomix/skv"
)

func (cn *Connector) PvNew(path string, value interface{}, opts *skv.ProgWriteOptions) skv.Result {
	return cn.ProgNew(pv_path_parser(path), skv.NewValueObject(value), opts)
}

func (cn *Connector) PvDel(path string, opts *skv.ProgWriteOptions) skv.Result {
	return cn.ProgDel(pv_path_parser(path), opts)
}

func (cn *Connector) PvPut(path string, value interface{}, opts *skv.ProgWriteOptions) skv.Result {
	return cn.ProgPut(pv_path_parser(path), skv.NewValueObject(value), opts)
}

func (cn *Connector) PvGet(path string) skv.Result {
	return cn.ProgGet(pv_path_parser(path))
}

func (cn *Connector) PvScan(fold, offset, cutset string, limit int) skv.Result {
	return cn.ProgScan(pv_path_parser_add(fold, offset), pv_path_parser_add(fold, cutset), limit)
}

func (cn *Connector) PvRevScan(fold, offset, cutset string, limit int) skv.Result {
	return cn.ProgRevScan(pv_path_parser_add(fold, offset), pv_path_parser_add(fold, cutset), limit)
}

func pv_path_parser(path string) skv.ProgKey {
	values := strings.Split(pv_path_clean(path), "/")
	k := skv.ProgKey{}
	for _, value := range values {
		k.Append(value)
	}
	return k
}

func pv_path_parser_add(path, add string) skv.ProgKey {
	k := pv_path_parser(path)
	k.Append(add)
	return k
}

func pv_path_clean(path string) string {
	return strings.Trim(strings.Trim(filepath.Clean(path), "/"), ".")
}
