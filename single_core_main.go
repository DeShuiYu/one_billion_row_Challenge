package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func processFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	statsMap := make(map[string]*Stats)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
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

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	fmt.Print("{")
	for station, stat := range statsMap {
		mean := stat.Sum / float64(stat.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", station, stat.Min, mean, stat.Max)
	}
	fmt.Print("\b\b} \n")
}

func main() {
	processFile("data/measurements.txt")
}
