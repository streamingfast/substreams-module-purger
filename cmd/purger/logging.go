package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.ApplicationLogger("substreams-module-purger", "github.com/streamingfast/network-size-calculator")

func init() {
	cli.SetLogger(zlog, tracer)
}
