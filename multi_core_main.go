package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func worker(lines []string, wg *sync.WaitGroup, statsChan chan map[string]*Stats) {
	defer wg.Done()
	statsMap := make(map[string]*Stats)

	for _, line := range lines {
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			continue
		}

		station := parts[0]
		measurement, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		stat, exists := statsMap[station]
		if !exists {
			statsMap[station] = &Stats{
				Min:   measurement,
				Max:   measurement,
				Sum:   measurement,
				Count: 1,
			}
		} else {
			if measurement < stat.Min {
				stat.Min = measurement
			}
			if measurement > stat.Max {
				stat.Max = measurement
			}
			stat.Sum += measurement
			stat.Count++
		}
	}

	statsChan <- statsMap
}

func processFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	numCPU := runtime.NumCPU()
	linesPerWorker := 1000000
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0, linesPerWorker)

	var wg sync.WaitGroup
	statsChan := make(chan map[string]*Stats, numCPU)

	go func() {
		wg.Wait()
		close(statsChan)
	}()

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= linesPerWorker {
			wg.Add(1)
			go worker(lines, &wg, statsChan)
			lines = make([]string, 0, linesPerWorker)
		}
	}

	if len(lines) > 0 {
		wg.Add(1)
		go worker(lines, &wg, statsChan)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	finalStats := make(map[string]*Stats)
	for partialStats := range statsChan {
		for station, stat := range partialStats {
			existingStat, exists := finalStats[station]
			if !exists {
				finalStats[station] = stat
			} else {
				if stat.Min < existingStat.Min {
					existingStat.Min = stat.Min
				}
				if stat.Max > existingStat.Max {
					existingStat.Max = stat.Max
				}
				existingStat.Sum += stat.Sum
				existingStat.Count += stat.Count
			}
		}
	}

	fmt.Print("{")
	for station, stat := range finalStats {
		mean := stat.Sum / float64(stat.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", station, stat.Min, mean, stat.Max)
	}
	fmt.Print("\b\b} \n")
}

func main() {
	processFile("data/measurements.txt")
}
