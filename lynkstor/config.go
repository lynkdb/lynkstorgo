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
	"github.com/lynkdb/iomix/connect"
)

type Config struct {

	// Database server hostname or IP. Leave blank if using unix sockets
	Host string `json:"host"`

	// Database server port. Leave blank if using unix sockets
	Port uint16 `json:"port"`

	// Password for authentication
	Auth string `json:"auth"`

	// A path of a UNIX socket file. Leave blank if using host and port
	Socket string `json:"socket"`

	// The connection timeout to a redis host (seconds)
	Timeout int `json:"timeout"`

	// Maximum number of connections
	MaxConn int `json:"maxconn"`
}

func NewConfig(copts connect.ConnOptions) Config {

	// if err := copts.Name.Valid(); err != nil {
	// 	return nil, errors.New("Invalid Connector Name: " + err.Error())
	// }

	cfg := Config{}

	if v, ok := copts.Items.Get("host"); ok {
		cfg.Host = v.String()
	}

	if v, ok := copts.Items.Get("port"); ok {
		cfg.Port = v.Uint16()
	}

	if v, ok := copts.Items.Get("auth"); ok {
		cfg.Auth = v.String()
	}

	if v, ok := copts.Items.Get("socket"); ok {
		cfg.Socket = v.String()
	}

	if v, ok := copts.Items.Get("timeout"); ok {
		cfg.Timeout = v.Int()
	}

	if v, ok := copts.Items.Get("max_conn"); ok {
		cfg.MaxConn = v.Int()
	}

	return cfg
}
