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

func worker(id int, jobs <-chan []string, results chan<- map[string]*Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	for lines := range jobs {
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
		results <- statsMap
	}
}

func processFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	numCPU := runtime.NumCPU()
	jobs := make(chan []string, numCPU)
	results := make(chan map[string]*Stats, numCPU)

	var wg sync.WaitGroup
	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	scanner := bufio.NewScanner(file)
	bufferSize := 1000000
	lines := make([]string, 0, bufferSize)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= bufferSize {
			jobs <- lines
			lines = make([]string, 0, bufferSize)
		}
	}

	if len(lines) > 0 {
		jobs <- lines
	}
	close(jobs)

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	finalStats := make(map[string]*Stats)
	for partialStats := range results {
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
