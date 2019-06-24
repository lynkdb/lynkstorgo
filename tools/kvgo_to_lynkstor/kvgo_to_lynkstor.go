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

package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hooto/hflag4g/hflag"
	"github.com/lessos/lessgo/types"
	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/skv"
	"github.com/lynkdb/kvgo"

	"github.com/lynkdb/lynkstorgo/lynkstor"
)

func main() {

	data_dir, ok := hflag.Value("src_dir")
	if !ok {
		log.Fatal("no --src_dir found")
	}
	if fp, err := os.Open(data_dir.String() + "/CURRENT"); err != nil {
		log.Fatal("invalid --src_dir")
	} else {
		fp.Close()
	}

	src_cfg := connect.ConnOptions{
		Name:      "lynkdb/kvgo",
		Connector: "iomix/skv/Connector",
	}
	src_cfg.Items.Set("data_dir", data_dir.String())

	kvdb, err := kvgo.Open(src_cfg)
	if err != nil {
		log.Fatal(err)
	}

	dst_host := "127.0.0.1"
	dst_port := "6378"
	dst_auth := ""
	dst_zone := ""
	dst_nss := types.ArrayString{}

	if v, ok := hflag.Value("dst_host"); ok {
		dst_host = v.String()
	}
	if v, ok := hflag.Value("dst_port"); ok {
		dst_port = v.String()
	}
	if v, ok := hflag.Value("dst_auth"); ok {
		dst_auth = v.String()
	}
	if v, ok := hflag.Value("dst_auth"); ok {
		dst_auth = v.String()
	}
	if v, ok := hflag.Value("dst_zone"); ok {
		dst_zone = v.String()
	}

	if dst_zone == "" {
		print_err(errors.New("not dst_zone found"))
		return
	}

	dst_cfg := connect.ConnOptions{
		Name:      "lynkstor/go/test",
		Connector: "iomix/skv/Connector",
	}
	dst_cfg.Items.Set("host", dst_host)
	dst_cfg.Items.Set("port", dst_port)
	dst_cfg.Items.Set("auth", dst_auth)

	fmt.Println("Connect")
	conn, err := lynkstor.NewConnector(lynkstor.NewConfig(dst_cfg))
	if err != nil {
		print_err(err.Error())
		return
	} else {
		print_ok("OK")
	}
	defer conn.Close()

	offset := []byte{uint8(8)}
	cutset := []byte{uint8(8)}
	limit := 1000
	for {
		rs := kvdb.RawScan(offset, cutset, limit)
		for _, v := range rs.KvList() {
			fmt.Println(v.Key)
			offset = v.Key
		}
		if rs.KvLen() < limit {
			break
		}
	}

	offset = []byte{uint8(36)}
	cutset = []byte{uint8(36)}
	num_ok, num_er, num_bytes, num_json, num_prog := 0, 0, 0, 0, 0
	prefixs := types.ArrayString{}

	for {
		rs := kvdb.RawScan(offset, cutset, limit)
		for _, v := range rs.KvList() {
			offset = v.Key

			value := skv.ValueBytes(v.Value).Bytes()
			if len(value) < 1 {
				log.Fatal("value error")
			}

			key := bytes_clone(v.Key)

			if n := strings.Index(string(key), "local"); n >= 0 {
				if n+5 >= len(key) {
					key = append(bytes_clone(key[:n]), []byte(dst_zone)...)
				} else {
					key[n-1] = uint8(len(dst_zone))
					key2 := append(append(bytes_clone(key[:n]), []byte(dst_zone)...), bytes_clone(key[n+5:])...)
					key = bytes_clone(key2)
				}
			}

			if k := skv.ProgKeyDecode(key); k == nil {
				log.Fatal("invalid key")
			} else {
				prefixs.Set(string(k.Items[0].Data))
				if !dst_nss.Has(string(k.Items[0].Data)) {
					continue
				}
			}

			// print_prog_key(key)

			switch value[0] {

			case 0:
				value[0] = 0x00
				num_bytes++

			case 20:
				value[0] = uint8(20)
				num_json++

				js := strings.Replace(string(value[1:]), "\"local\"", "\""+dst_zone+"\"", -1)
				js = strings.Replace(js, "zm/local", "zm/"+dst_zone, -1)

				value = []byte{}
				value = append([]byte{uint8(20)}, []byte(js)...)

			case 32:
				num_prog++
				fmt.Println()
				print_prog_key(v.Key)
				fmt.Println(v.Value)
				fmt.Println()
				continue

			default:
				fmt.Println(value)
				log.Fatal("invalid type")
			}

			meta2 := &skv.KvMeta{}
			meta := v.Meta()
			if meta != nil {
				if meta.Expired > 0 {
					meta2.Expired = meta.Expired / 1e6
				}
			}
			meta_enc, err := proto.Marshal(meta2)
			if err != nil {
				log.Fatal(err)
			}

			enc := []byte{0x01, 0}
			enc[1] = uint8(len(meta_enc))
			if len(meta_enc) > 0 {
				enc = append(enc, meta_enc...)
			}
			enc = append(enc, value...)

			drs := conn.Cmd("rep_data_put", key, enc)
			if drs.OK() {
				num_ok++
			} else {
				num_er++
			}
		}
		if rs.KvLen() < limit {
			break
		}
	}
	fmt.Println("OK", num_ok, "ER", num_er, "bytes", num_bytes, "prog", num_prog, "json", num_json)
	// fmt.Println(prefixs)
}

func bytes_clone(src []byte) []byte {

	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

var creg = regexp.MustCompile(`([0-9a-zA-Z.]{1,60})`)

func print_prog_key(key []byte) {

	k := skv.ProgKeyDecode(key)
	if k == nil {
		fmt.Println("invalid prog key")
		return
	}
	for _, v := range k.Items {
		if creg.MatchString(string(v.Data)) {
			fmt.Printf(" / %s", string(v.Data))
		} else {
			fmt.Printf(" / %v", v.Data)
		}
	}
	fmt.Println()
}

func print_ok(msg string) {
	fmt.Printf("\033[32m  %s \033[0m\n", msg)
}

func print_err(msg string) {
	fmt.Printf("\033[31m  %s \033[0m\n", msg)
}
