package main

import (
	"log"
	"net/http"
)

func main() {

	go func() {
		err := http.ListenAndServe(":6060", nil)
		if err != nil {
			zlog.Debug("unable to start profiling server")
		}
	}()

	if err := purgerCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
