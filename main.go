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
	mp := make(map[string]*stationData)

	f, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("Error while opening the file, %v", err)
	}
	defer f.Close()

	buffer := func() <-chan []string {
		stream := make(chan []string, 100)
		sendBuffer := make([]string, 100)

		// 100 chunk size to read
		chunkSize := 100 * 1024 * 1024
		buf := make([]byte, chunkSize)
		var builder strings.Builder
		builder.Grow(1024)

		var cnt int
		go func() {
			defer close(stream)

			for {
				read, err := f.Read(buf)
				if err != nil {
					if errors.Is(err, io.EOF) {
						cnt = readChunk(buf, read, cnt, &builder, sendBuffer, stream)
						break
					}
					log.Fatalf("error occuered while reading the chunk from file.\n %v\n", err)
				}
				cnt = readChunk(buf, read, cnt, &builder, sendBuffer, stream)
			}
			if cnt != 0 {
				stream <- sendBuffer[:cnt]
			}
		}()
		return stream
	}

	stream := buffer()

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

func readChunk(buf []byte, read, cnt int, builder *strings.Builder, sendBuffer []string, stream chan<- []string) int {
	for _, ch := range buf[:read] {
		if ch == '\n' {
			if builder.Len() != 0 {
				sendBuffer[cnt] = builder.String()
				builder.Reset()
				cnt++

				if cnt == 100 {
					cnt = 0
					hereCopy := make([]string, 100)
					copy(hereCopy, sendBuffer)
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
