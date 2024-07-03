package main

import (
	"bufio"
	"fmt"
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

	buffer := func() <-chan [][]string {
		stream := make(chan [][]string, 100)
		sendBuffer := make([][]string, 1024)
		go func() {
			defer close(stream)

			scanner := bufio.NewScanner(f)
			bIdx := 0
			for scanner.Scan() {
				if bIdx == 1024 {
					hereCopy := make([][]string, 1024)
					copy(hereCopy, sendBuffer)
					stream <- hereCopy
					bIdx = 0
				}
				sendBuffer[bIdx] = strings.Split(scanner.Text(), ";")
				bIdx++
			}
			if bIdx != 0 {
				stream <- sendBuffer[:bIdx]
			}
		}()
		return stream
	}

	stream := buffer()

	for chunk := range stream {
		for _, line := range chunk {
			city, tempStr := line[0], line[1]

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
