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
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/lynkdb/iomix/skv"
)

type Connector struct {
	clients chan *client
	cfg     Config
	copts   *connOptions
}

type connOptions struct {
	net     string
	addr    string
	timeout time.Duration
	auth    string
}

func NewConnector(cfg Config) (*Connector, error) {

	if cfg.MaxConn < 1 {
		cfg.MaxConn = 1
	} else {
		maxconn := runtime.NumCPU()
		if maxconn > 10 {
			maxconn = 10
		}
		if cfg.MaxConn > maxconn {
			cfg.MaxConn = maxconn
		}
	}

	if cfg.Timeout < 3 {
		cfg.Timeout = 3
	} else if cfg.Timeout > 600 {
		cfg.Timeout = 600
	}

	opts := &connOptions{
		timeout: time.Duration(cfg.Timeout) * time.Second,
		auth:    cfg.Auth,
	}

	if len(cfg.Socket) > 2 {
		if _, err := net.ResolveUnixAddr("unix", cfg.Socket); err == nil {
			opts.net, opts.addr = "unix", cfg.Socket
		}
	}

	if opts.net == "" {
		opts.addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		if _, err := net.ResolveTCPAddr("tcp", opts.addr); err != nil {
			return nil, err
		}
		opts.net = "tcp"
	}

	c := &Connector{
		clients: make(chan *client, cfg.MaxConn),
		cfg:     cfg,
		copts:   opts,
	}

	for i := 0; i < cfg.MaxConn; i++ {
		cli, err := newClient(c.copts)
		if err != nil {
			return c, err
		}
		c.clients <- cli
	}

	return c, nil
}

func (c *Connector) Cmd(cmd string, args ...interface{}) skv.Result {
	cli, _ := c.pull()
	defer c.push(cli)

	return cli.cmd(cmd, args...)
}

func (c *Connector) Close() error {
	for i := 0; i < c.cfg.MaxConn; i++ {
		cli, _ := c.pull()
		cli.Close()
	}
	return nil
}

func (c *Connector) push(cli *client) {
	c.clients <- cli
}

func (c *Connector) pull() (cli *client, err error) {
	return <-c.clients, nil
}
