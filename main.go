package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	f, err := os.Create(".profile/cpu-profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	start := time.Now()
	doStuff()
	fmt.Printf("Time it took since start: %v seconds\n", time.Now().Sub(start))
}

type stationData struct {
	min float32
	max float32
	sum float32
	cnt int
}

func doStuff() {
	f, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("Error while opening the file, %v", err)
	}
	defer f.Close()

	buffer := func() <-chan []string {
		stream := make(chan []string, 128)
		bufferSlice := make([]string, 128)

		// 4MB chunk size to read
		chunkSize := 4 * 1024 * 1024
		readChunk := make([]byte, chunkSize)
		var builder strings.Builder
		builder.Grow(128)

		var cnt int
		go func() {
			defer close(stream)

			for {
				read, err := f.Read(readChunk)
				if err != nil {
					if errors.Is(err, io.EOF) {
						cnt = processChunk(readChunk, read, cnt, &builder, bufferSlice, stream)
						break
					}
					log.Fatalf("error occuered while reading the chunk from file.\n %v\n", err)
				}
				cnt = processChunk(readChunk, read, cnt, &builder, bufferSlice, stream)
			}
			if cnt != 0 {
				stream <- bufferSlice[:cnt]
			}
		}()
		return stream
	}

	stream := buffer()

	mp := make(map[string]*stationData)
	for chunk := range stream {
		for _, line := range chunk {
			idx := strings.Index(line, ";")
			// idk why this should ever happen, but still got it checked
			if idx == -1 {
				continue
			}

			city, tempStr := line[:idx], line[idx+1:]
			temp64, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				log.Fatalf("failed to convert %s into float", tempStr)
			}
			temp := float32(temp64)

			station, ok := mp[city]
			if !ok {
				mp[city] = &stationData{temp, temp, temp, 1}
			} else {
				if temp < station.min {
					station.min = temp
				} else if temp > station.max {
					station.max = temp
				}
				station.sum += temp
				station.cnt++
			}
		}
	}
	printStuff(mp)
}

func processChunk(readBuffer []byte, read, cnt int, builder *strings.Builder, bufferSlice []string, stream chan<- []string) int {
	for _, ch := range readBuffer[:read] {
		if ch == '\n' {
			if builder.Len() != 0 {
				bufferSlice[cnt] = builder.String()
				builder.Reset()
				cnt++

				if cnt == 128 {
					cnt = 0
					hereCopy := make([]string, 128)
					copy(hereCopy, bufferSlice)
					stream <- hereCopy
				}
			}
		} else {
			builder.WriteByte(ch)
		}
	}
	return cnt
}

func printStuff(mp map[string]*stationData) {
	cities := make([]string, 0, len(mp))

	for key := range mp {
		cities = append(cities, key)
	}

	sort.Strings(cities)

	print("{\n")
	for _, city := range cities {
		val := mp[city]
		fmt.Printf("%s=%0.1f/%0.1f/%0.1f\n", city, val.min, val.sum/float32(val.cnt), val.max)
	}
	print("}\n")
}
