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
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/lessos/lessgo/types"
	"github.com/lynkdb/iomix/skv"
)

func (cn *Connector) FoMpInit(sets skv.FileObjectEntryInit) skv.Result {
	if !sets.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&sets)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("fompinit", bs)
}

func (cn *Connector) FoMpPut(sets skv.FileObjectEntryBlock) skv.Result {
	if !sets.Valid() {
		return newResult(skv.ResultBadArgument, nil)
	}
	bs, err := proto.Marshal(&sets)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("fompput", bs)
}

func (cn *Connector) FoMpGet(sets skv.FileObjectEntryBlock) skv.Result {
	bs, err := proto.Marshal(&sets)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}
	return cn.Cmd("fompget", bs)
}

func (cn *Connector) FoGet(path_key string) skv.Result {
	return cn.Cmd("foget", skv.FileObjectPathEncode(path_key))
}

func (cn *Connector) FoScan(offset, cutset string, limit int) skv.Result {
	return cn.Cmd("foscan", skv.FileObjectPathEncode(offset), skv.FileObjectPathEncode(cutset), limit)
}

func (cn *Connector) FoRevScan(offset, cutset string, limit int) skv.Result {
	return cn.Cmd("forevscan", skv.FileObjectPathEncode(offset), skv.FileObjectPathEncode(cutset), limit)
}

func (cn *Connector) FoFilePut(src_path, dst_path string) skv.Result {

	fp, err := os.Open(src_path)
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}

	st, err := fp.Stat()
	if err != nil {
		return newResult(skv.ResultBadArgument, err)
	}

	if st.Size() < 1 {
		return newResult(skv.ResultBadArgument, errors.New("invalid file size"))
	}

	mp_init := skv.NewFileObjectEntryInit(dst_path, uint64(st.Size()))
	rs := cn.FoMpInit(mp_init)
	if !rs.OK() {
		return rs
	}

	var fo_meta skv.FileObjectEntryMeta
	if err := rs.Decode(&fo_meta); err != nil {
		return newResult(skv.ResultBadArgument, err)
	}

	if fo_meta.Size != uint64(st.Size()) {
		return newResult(skv.ResultBadArgument, errors.New("protocol error"))
	}

	if !fo_meta.AttrAllow(skv.FileObjectEntryAttrCommiting) {
		return newResult(skv.ResultOK, nil)
	}

	block_size := uint64(0)
	if fo_meta.AttrAllow(skv.FileObjectEntryAttrBlockSize4) {
		block_size = skv.FileObjectBlockSize4
	}

	if block_size == 0 {
		return newResult(skv.ResultBadArgument, errors.New("protocol error"))
	}

	block_dones := types.ArrayUint32(fo_meta.Blocks)

	num := uint32(fo_meta.Size / block_size)
	if num > 0 && (fo_meta.Size%block_size) == 0 {
		num -= 1
	}
	// fmt.Println("block num ", num)

	for n := uint32(0); n <= num; n++ {
		if block_dones.Has(n) {
			continue
		}

		bsize := int(block_size)
		if n == num {
			bsize = int(fo_meta.Size % block_size)
		}

		bs := make([]byte, bsize)
		if rn, err := fp.ReadAt(bs, int64(n)*int64(block_size)); err != nil {
			return newResult(skv.ResultBadArgument, err)
		} else if rn != bsize {
			return newResult(skv.ResultBadArgument, errors.New("io error"))
		} else {
			mp_block := skv.NewFileObjectEntryBlock(dst_path, fo_meta.Size, n, bs, fo_meta.CommitKey)
			mp_block.Sum = uint64(crc32.ChecksumIEEE(bs))
			if rs = cn.FoMpPut(mp_block); !rs.OK() {
				return rs
			}
		}
	}

	return newResult(skv.ResultOK, nil)
}

type FoReadSeeker struct {
	conn      *Connector
	db_meta   *skv.KvMeta
	fo_meta   skv.FileObjectEntryMeta
	path      string
	offset    int64
	cur_block *skv.FileObjectEntryBlock
}

func (fo *FoReadSeeker) Seek(offset int64, whence int) (int64, error) {

	abs := int64(0)

	switch whence {
	case 0:
		abs = offset

	case 1:
		abs = fo.offset + offset

	case 2:
		abs = offset + int64(fo.fo_meta.Size)

	default:
		return 0, errors.New("invalid seek whence")
	}

	if abs < 0 {
		return 0, errors.New("out range of size")
	}
	fo.offset = abs

	return fo.offset, nil
}

func (fo *FoReadSeeker) Read(b []byte) (n int, err error) {

	if len(b) == 0 {
		return 0, nil
	}

	block_size := int64(0)
	if fo.fo_meta.AttrAllow(skv.FileObjectEntryAttrBlockSize4) {
		block_size = int64(skv.FileObjectBlockSize4)
	}
	if block_size == 0 {
		return 0, errors.New("protocol error")
	}

	blk_num_max := uint32(fo.fo_meta.Size / uint64(block_size))
	if (fo.fo_meta.Size % uint64(block_size)) > 0 {
		blk_num_max += 1
	}

	var (
		n_done = 0
		n_len  = len(b)
	)

	// fmt.Println("Read", fo.path, "len", len(b), "fo.offset", fo.offset, "max num", blk_num_max)

	for {

		if fo.offset >= int64(fo.fo_meta.Size) {
			return n_done, io.EOF
		}

		var (
			blk_num = uint32(fo.offset / block_size)
			blk_off = int(fo.offset % block_size)
		)

		if blk_num > blk_num_max {
			return n_done, io.EOF
		}

		// fmt.Println("Read", fo.path, "fo.offset", fo.offset, "num", blk_num)

		if fo.cur_block == nil || fo.cur_block.Num != blk_num {

			blk_block := skv.NewFileObjectEntryBlock(fo.path, 0, blk_num, nil, "")
			blk_block.Sn = fo.fo_meta.Sn
			rs := fo.conn.FoMpGet(blk_block)
			if !rs.OK() {
				return 0, errors.New("io error")
			}

			var fo_block skv.FileObjectEntryBlock
			if err := rs.Decode(&fo_block); err != nil {
				return 0, errors.New("io error")
			}

			if len(fo_block.Data) < 1 {
				return 0, errors.New("io error")
			}

			fo.cur_block = &fo_block

			// fmt.Println("Read", fo.path, "fo.offset", fo.offset, "num", blk_num, "data", len(fo.cur_block.Data))
		}

		blk_off_n := len(fo.cur_block.Data) - blk_off
		if blk_off_n < 1 {
			return 0, errors.New("offset error")
		}
		if blk_off_n > n_len {
			blk_off_n = n_len
		}

		// fmt.Println("Read", fo.path, "fo.offset", fo.offset, "num", blk_num,
		// 	"copy", n_done, "to",
		// 	blk_off, blk_off_n)

		copy(b[n_done:], fo.cur_block.Data[blk_off:(blk_off+blk_off_n)])

		fo.offset += int64(blk_off_n)
		n_done += blk_off_n
		n_len -= blk_off_n

		if n_len < 1 {
			break
		}
	}

	return n_done, nil
}

func (cn *Connector) FoFileOpen(path string) (io.ReadSeeker, error) {

	rs := cn.FoGet(path)
	if !rs.OK() {
		return nil, errors.New(rs.String())
	}
	rs_meta := rs.Meta()
	if rs_meta == nil {
		return nil, errors.New("ER no meta found")
	}

	var fo_meta skv.FileObjectEntryMeta
	if err := rs.Decode(&fo_meta); err != nil {
		return nil, errors.New("ER decode meta : " + err.Error())
	}

	// fmt.Println("FoFileOpen", path)
	return &FoReadSeeker{
		conn:    cn,
		db_meta: rs_meta,
		fo_meta: fo_meta,
		path:    path,
		offset:  0,
	}, nil
}
