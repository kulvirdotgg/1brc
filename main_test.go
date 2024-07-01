package main

import (
	"os"
	"testing"
)

func TestRandom(t *testing.T) {
	hs, _ := os.Hostname()
	wd, _ := os.Getwd()
	t.Logf(hs, ": ", wd)
}
