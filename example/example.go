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
	"hash/crc32"
	"time"

	"github.com/lessos/lessgo/encoding/json"
	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/skv"

	"code.hooto.com/lynkdb/lynkstorgo/lynkstor"
)

func main() {

	conn_cfg := connect.ConnOptions{
		Name:      "lynkstor/go/test",
		Connector: "iomix/skv/Connector",
	}
	conn_cfg.Items.Set("host", "127.0.0.1")
	conn_cfg.Items.Set("port", "5559")
	conn_cfg.Items.Set("timeout", "3")

	fmt.Println("Connect")
	conn, err := lynkstor.NewConnector(lynkstor.NewConfig(conn_cfg))
	if err != nil {
		print_err(err.Error())
		return
	} else {
		print_ok("OK")
	}
	defer conn.Close()

	fmt.Println()

	{
		if false {
			// src_file := "/home/eryx/CCTV1/CCTV1_512000_20180317_27852677_51.mp4"
			// dst_object := "/bucket/demo/02.mp4"
			src_file := "/home/eryx/item_jd.zip"
			dst_object := "/abc/abc/abc.zip"
			// src_file := "/opt/gopath/src/github.com/sysinner/insoho/var/tmp/git-2.15.1-3.el7.x64.txz"
			// dst_object := "/git/2.15.1/git-2.15.1-3.el7.x64.txz"
			if rs := conn.FoFilePut(src_file, dst_object); !rs.OK() {
				print_err("ER " + rs.String())
			} else {
				print_ok("OK")
			}
			return
		}

		mp_block_sn := uint32(0)
		for i := 0; i < 2; i++ {
			fmt.Println("FO MP INIT", i)
			mp_init := skv.NewFileObjectEntryInit(fmt.Sprintf("/abc/123-%d", i), 10)
			if rs := conn.FoMpInit(mp_init); !rs.OK() {
				print_err("ER " + rs.String())
			} else {
				print_ok("OK")

				rs_meta := rs.Meta()
				if rs_meta == nil {
					print_err("ER no meta found")
				} else {
					var fo_meta skv.FileObjectEntryMeta
					if err := rs.Decode(&fo_meta); err != nil {
						print_err("ER decode meta : " + err.Error())
					} else {
						rs_js, _ := json.Encode(rs_meta, "  ")
						fo_js, _ := json.Encode(fo_meta, "  ")
						print_ok(fmt.Sprintf("OK %s /// %s", string(rs_js), string(fo_js)))
						if i == 0 {
							mp_block_sn = fo_meta.Sn
						}
					}
				}
			}

			fmt.Println("FO MP PUT", i)
			mp_block := skv.NewFileObjectEntryBlock(fmt.Sprintf("/abc/123-%d", i), 10, 0, []byte("0123456789"), "")
			if rs := conn.FoMpPut(mp_block); !rs.OK() {
				print_err("ER " + rs.String())
				fmt.Println(rs.Status())
			} else {
				print_ok("OK")
			}
		}

		fmt.Println("FO MP GET")
		mp_block := skv.NewFileObjectEntryBlock("/abc/123-0", 0, 0, nil, "")
		mp_block.Sn = mp_block_sn
		if rs := conn.FoMpGet(mp_block); !rs.OK() {
			print_err("ER " + rs.String())
			fmt.Println(rs.Status())
		} else {
			print_ok("OK ")
			rs_meta := rs.Meta()
			if rs_meta == nil {
				print_err("ER no meta found")
			} else {
				var fo_block skv.FileObjectEntryBlock
				if err := rs.Decode(&fo_block); err != nil {
					print_err("ER decode meta : " + err.Error())
				} else {
					rs_js, _ := json.Encode(rs_meta, "  ")
					fo_js, _ := json.Encode(fo_block, "  ")
					print_ok(fmt.Sprintf("OK %s /// %s", string(rs_js), string(fo_js)))
					print_ok("OK DATA {{{" + string(fo_block.Data) + "}}}")
				}
			}
		}

		fmt.Println("FO GET")
		if rs := conn.FoGet("/abc/123-0"); !rs.OK() {
			print_err("ER " + rs.String())
			fmt.Println(rs.Status())
		} else {
			print_ok("OK ")
			rs_meta := rs.Meta()
			if rs_meta == nil {
				print_err("ER no meta found")
			} else {
				var fo_meta skv.FileObjectEntryMeta
				if err := rs.Decode(&fo_meta); err != nil {
					print_err("ER decode meta : " + err.Error())
				} else {
					rs_js, _ := json.Encode(rs_meta, "  ")
					fo_js, _ := json.Encode(fo_meta, "  ")
					print_ok(fmt.Sprintf("OK %s /// %s", string(rs_js), string(fo_js)))
				}
			}
		}

		fmt.Println("FO SCAN")
		if rs := conn.FoScan("/abc/", "/abc/", 10); !rs.OK() {
			print_err("ER " + rs.String())
		} else {
			ls := rs.KvPairs()
			print_ok(fmt.Sprintf("OK num:%d", len(ls)))
			for i, v := range ls {
				print_ok(fmt.Sprintf("OK i:%d, k:%s", i, v.KvKey()))

				rs_meta := v.Meta()
				if rs_meta == nil {
					print_err("ER no meta found")
				} else {
					var fo_meta skv.FileObjectEntryMeta
					if err := v.Decode(&fo_meta); err != nil {
						print_err("ER decode meta : " + err.Error())
					} else {
						rs_js, _ := json.Encode(rs_meta, "  ")
						fo_js, _ := json.Encode(fo_meta, "  ")
						print_ok(fmt.Sprintf("OK\nrs_meta: %s\nfo_meta: %s", string(rs_js), string(fo_js)))
					}
				}
			}
		}

		fmt.Println("FO REVSCAN")
		if rs := conn.FoRevScan("/abc/", "/abc/", 10); !rs.OK() {
			print_err("ER " + rs.String())
		} else {
			ls := rs.KvPairs()
			print_ok(fmt.Sprintf("OK num:%d", len(ls)))
			for i, v := range ls {
				print_ok(fmt.Sprintf("OK i:%d, k:%s", i, v.KvKey()))

				rs_meta := v.Meta()
				if rs_meta == nil {
					print_err("ER no meta found")
				} else {
					var fo_meta skv.FileObjectEntryMeta
					if err := v.Decode(&fo_meta); err != nil {
						print_err("ER decode meta : " + err.Error())
					} else {
						rs_js, _ := json.Encode(rs_meta, "  ")
						fo_js, _ := json.Encode(fo_meta, "  ")
						print_ok(fmt.Sprintf("OK\nrs_meta: %s\nfo_meta: %s", string(rs_js), string(fo_js)))
					}
				}
			}
		}

		return
	}

	{
		k := skv.NewKvProgKey("iam", "afm", "")
		if rs := conn.KvProgRevScan(k, k, 1000); rs.OK() {
			fmt.Println("len ", len(rs.KvList()))
		}

		fmt.Println("KV PROG PUT")
		if rs := conn.KvProgPut(skv.NewKvProgKey("abc", "def"), skv.NewKvEntry("value-of"), nil); !rs.OK() {
			print_err("ER " + rs.String())
		} else {
			print_ok("OK")
		}

		fmt.Println("KV PROG NEW")
		if rs := conn.KvProgNew(skv.NewKvProgKey("abc", "def"), skv.NewKvEntry("value-of-000"), nil); rs.OK() && rs.Int() == 0 {
			print_ok("OK")
		} else {
			print_err("ER " + rs.String())
		}

		fmt.Println("KV PROG PUT,PrevSum")
		if rs := conn.KvProgPut(skv.NewKvProgKey("abc", "def"), skv.NewKvEntry("value-of-2"), &skv.KvProgWriteOptions{
			PrevSum: crc32.ChecksumIEEE([]byte("value-error")),
		}); !rs.OK() {
			print_ok("OK 1")
		} else {
			print_err("ER 1 " + rs.String())
		}
		if rs := conn.KvProgPut(skv.NewKvProgKey("abc", "def"), skv.NewKvEntry("value-of-2"), &skv.KvProgWriteOptions{
			PrevSum: crc32.ChecksumIEEE([]byte("value-of")),
		}); rs.OK() {
			print_ok("OK 2")
		} else {
			print_err("ER 2 " + rs.String())
		}

		fmt.Println("KV PROG GET")
		if rs := conn.KvProgGet(skv.NewKvProgKey("abc", "def")); rs.OK() && rs.String() == "value-of-2" {
			print_ok("OK")
		} else {
			print_err("ER " + rs.String())
		}

		fmt.Println("KV PROG DEL")
		if rs := conn.KvProgDel(skv.NewKvProgKey("abc", "def"), nil); rs.OK() {
			print_ok("OK")
		} else {
			print_err("ER " + rs.String())
		}

		fmt.Println("KV PROG DEL/GET")
		if rs := conn.KvProgGet(skv.NewKvProgKey("abc", "def")); rs.NotFound() {
			print_ok("OK")
		} else {
			print_err("ER " + rs.String())
		}

		fmt.Println("KV PROG PUT + INCR")
		conn.KvProgPut(skv.NewKvProgKey("abc", "incr"), skv.NewKvEntry(10), nil)
		if rs := conn.KvProgGet(skv.NewKvProgKey("abc", "incr")); rs.OK() && rs.Int() == 10 {
			print_ok("OK " + rs.String())
		} else {
			print_err("ERR " + rs.String())
		}
		if rs := conn.KvProgIncr(skv.NewKvProgKey("abc", "incr"), 1); rs.OK() && rs.Int() == 11 {
			print_ok("OK " + rs.String())
		} else {
			print_err("ERR " + rs.String())
		}

		fmt.Println("KV PROG TTL/META")
		conn.KvProgPut(skv.NewKvProgKey("abc", "ttl"), skv.NewKvEntry("value"), &skv.KvProgWriteOptions{
			Expired: uint64(time.Now().UnixNano()) + (3 * 1e9),
			Actions: skv.KvProgOpMetaSize | skv.KvProgOpMetaSum,
		})
		if rs := conn.KvProgGet(skv.NewKvProgKey("abc", "ttl")); rs.OK() && rs.String() == "value" {
			print_ok("OK " + rs.String())
		} else {
			print_err("ERR " + rs.String())
		}
		if rs := conn.KvProgMeta(skv.NewKvProgKey("abc", "ttl")); rs.OK() && rs.Meta() != nil {
			ttl := int64(rs.Meta().Expired) - (time.Now().UnixNano() / 1e6)
			js, _ := json.Encode(rs.Meta(), "  ")
			if rs.Meta().Size == 5 && rs.Meta().Sum == crc32.ChecksumIEEE([]byte("value")) && ttl <= 3000 && ttl > 1000 {
				print_ok("OK\n" + string(js))
			} else {
				print_err("ER\n" + string(js))
			}
		} else {
			print_err("ER " + rs.String())
		}
	}

	{
		fmt.Println("KVPUT API::Bool() bool")
		if rs := conn.KvPut([]byte("true"), "True", nil); !rs.OK() {
			print_err("Failed " + rs.String())
		}

		if rs := conn.KvGet([]byte("true")); rs.OK() && rs.Bool() {
			print_ok("Bool OK")
		} else {
			print_err("Bool Failed " + rs.String())
		}
	}

	{
		fmt.Println("SET API::String() string")
		conn.KvPut([]byte("aa"), "val-aaaaaaaaaaaaaaaaaa", nil)
		conn.KvPut([]byte("bb"), "val-bbbbbbbbbbbbbbbbbb", nil)
		conn.KvPut([]byte("cc"), "val-cccccccccccccccccc", nil)
		if rs := conn.KvGet([]byte("aa")); rs.OK() && rs.String() == "val-aaaaaaaaaaaaaaaaaa" {
			print_ok("OK (get by string) " + rs.String())
		} else {
			print_ok("ER " + rs.String())
		}
		if rs := conn.KvGet([]byte("aa")); rs.OK() && rs.String() == "val-aaaaaaaaaaaaaaaaaa" {
			print_ok("OK (get by bytes) " + rs.String())
		} else {
			print_ok("ER " + rs.String())
		}
	}

	{
		fmt.Println("SCAN")
		if rs := conn.KvScan([]byte("a"), []byte("zz"), 2); rs.OK() && rs.KvLen() == 2 {
			print_ok(fmt.Sprintf("OK multi len: %d", rs.KvLen()))
			ls := rs.KvList()
			for i, v := range ls {
				print_ok(fmt.Sprintf("  No. %d key:%s val:%s",
					i, string(v.Key), string(v.Value)))
			}
		} else {
			print_err("ER " + rs.String())
		}
	}

	{
		fmt.Println("DEL")
		conn.KvPut([]byte("key"), "aaa", nil)
		conn.KvPut([]byte("key2"), "aaa", nil)
		if rs := conn.KvDel([]byte("key"), []byte("key2")); rs.OK() && rs.Int() == 2 {
			print_ok("OK")
		} else {
			print_err("ERR " + rs.String())
		}
	}

	{
		fmt.Println("SET + INCRBY")
		conn.KvIncr([]byte("key"), 10)
		if rs := conn.KvIncr([]byte("key"), 1); rs.OK() && rs.Int() == 11 {
			print_ok("OK")
		} else {
			print_err("ERR " + rs.String())
		}
	}

	{
		fmt.Println("SET EX, TTL")
		conn.KvPut([]byte("key"), "test", &skv.KvWriteOptions{
			Ttl: 300000,
		})
		if rs_meta := conn.KvMeta([]byte("key")).Meta(); rs_meta != nil && rs_meta.Expired > 1512341234000 {
			print_ok("OK")
		} else {
			print_err("ERR")
		}
	}

	{
		fmt.Println("SET float")
		conn.KvPut([]byte("float"), 123.456, nil)
		if rs := conn.KvGet([]byte("float")).Float64(); rs == 123.456 {
			print_ok("OK")
		} else {
			print_err("ER")
		}
	}

	{
		fmt.Println("SET JSON")
		conn.KvPut([]byte("json_key"), "{\"name\": \"test obj.name\", \"value\": \"test obj.value\"}", nil)
		if rs := conn.KvGet([]byte("json_key")); rs.OK() {
			var rs_obj struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			}
			if err := rs.Decode(&rs_obj); err == nil {
				print_ok(fmt.Sprintf("OK key:%s value:%s", rs_obj.Name, rs_obj.Value))
			} else {
				print_err("ER " + err.Error())
			}
		} else {
			print_err("ER " + rs.String())
		}
	}
}

func print_ok(msg string) {
	fmt.Printf("\033[32m  %s \033[0m\n", msg)
}

func print_err(msg string) {
	fmt.Printf("\033[31m  %s \033[0m\n", msg)
}
