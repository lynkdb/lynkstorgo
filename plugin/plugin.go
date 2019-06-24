package main

import (
	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/skv"

	"github.com/lynkdb/lynkstorgo/lynkstor"
)

func NewConnector(copts *connect.ConnOptions) (skv.Connector, error) {
	return lynkstor.NewConnector(lynkstor.NewConfig(*copts))
}

func NewFileObjectConnector(copts *connect.ConnOptions) (skv.FileObjectConnector, error) {
	return lynkstor.NewConnector(lynkstor.NewConfig(*copts))
}
