package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"
)

type Measurements struct {
	Min, Max, Sum, Count float64
}

func main() {

	startTime := time.Now()
	// err := generateMeasurementFile(1000000000)
	// if err != nil {
	// 	fmt.Println(err)
	// 	panic("Error during file generation")
	// }
	// fmt.Printf("File generation executed in %v\n", time.Since(startTime))

	filePath := "measurements.txt"
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		panic("Error in file reading")
	}
	fileStats, err := os.Stat(filePath)
	if err != nil {
		fmt.Println(err)
		panic("Got error during file stats retrieval")
	}
	fileSize := fileStats.Size()

	results, err := processFile(file, fileSize)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		panic("Processing failed")
	}
	file.Close()

	displayResults(results)

	fmt.Printf("Processing executed in %v\n", time.Since(startTime))
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func convertBytesToFloat(bytes []byte) float64 {
	var startIndex int
	if bytes[0] == '-' {
		startIndex = 1
	}

	v := float64(bytes[len(bytes)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bytes) - 3; i >= startIndex; i-- { // integer part
		v += float64(bytes[i]-'0') * place
		place *= 10
	}

	if startIndex == 1 {
		v *= -1
	}
	return v
}

func processFile(file *os.File, fileSize int64) (map[string]*Measurements, error) {

	reader := bufio.NewReader(file)
	bufferSize := 30 * 1024 * 1024

	var wg sync.WaitGroup
	limiterCh := make(chan struct{}, 1000)
	resultsCh := make(chan map[string]*Measurements, 100000)
	errCh := make(chan error, 1000)
	buffer := make([]byte, bufferSize)

	for {
		n, err := io.ReadFull(reader, buffer)
		buf := buffer[:n]
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != io.ErrUnexpectedEOF {
				fmt.Println("Wasn't able to read chunk of the file")
				return nil, err
			}
		}

		nextUntillNewLine, err := reader.ReadBytes('\n')
		if err != io.EOF {
			buf = append(buf, nextUntillNewLine...)
		}

		limiterCh <- struct{}{}
		wg.Add(1)
		go processData(&wg, buf, limiterCh, resultsCh, errCh)
	}

	go func() {
		wg.Wait()
		close(limiterCh)
		close(resultsCh)
		close(errCh)
	}()

	result := aggregateMaps(resultsCh)

	// Collect and handle errors
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func processData(
	wg *sync.WaitGroup,
	data []byte,
	limiterCh chan struct{},
	resultsCh chan map[string]*Measurements,
	errCh chan error,
) {
	defer wg.Done()

	var result = make(map[string]*Measurements, 5000)
	lastStationName := make([]byte, 30)
	var lastStationLen int

	var index int
	for i := 0; i < len(data); i++ {
		if data[i] == ';' {
			stationName := data[index:i]
			lastStationLen = copy(lastStationName, stationName)
			index = i + 1
			continue
		} else if data[i] == '\n' {
			temperatureBytes := data[index:i]
			temperatureFloat := convertBytesToFloat(temperatureBytes)

			stationNameUnsafe := unsafe.String(&lastStationName[0], lastStationLen)
			existingStation, ok := result[stationNameUnsafe]
			if !ok {
				name := string(lastStationName[:lastStationLen])
				result[name] = &Measurements{
					Min:   temperatureFloat,
					Max:   temperatureFloat,
					Sum:   temperatureFloat,
					Count: 1.0,
				}
			} else {
				existingStation.Count += 1.0
				existingStation.Sum += temperatureFloat
				if temperatureFloat < existingStation.Min {
					existingStation.Min = temperatureFloat
				}
				if temperatureFloat > existingStation.Max {
					existingStation.Max = temperatureFloat
				}
			}
			index = i + 1
			continue
		}
	}

	resultsCh <- result
	<-limiterCh
}

func aggregateMaps(resultsCh chan map[string]*Measurements) map[string]*Measurements {
	finalMap := make(map[string]*Measurements, 200)
	for result := range resultsCh {
		for station, newMeasurement := range result {
			existentMeasurement, ok := finalMap[station]
			if !ok {
				finalMap[station] = newMeasurement
				continue
			}
			existentMeasurement.Count += newMeasurement.Count
			existentMeasurement.Sum += newMeasurement.Sum
			if newMeasurement.Min < existentMeasurement.Min {
				existentMeasurement.Min = newMeasurement.Min
			}
			if newMeasurement.Max > existentMeasurement.Max {
				existentMeasurement.Max = newMeasurement.Max
			}
		}
	}

	return finalMap
}

func displayResults(results map[string]*Measurements) {
	cities := make([]string, 0, len(results))
	for city := range results {
		cities = append(cities, city)
	}
	sort.Strings(cities)

	for _, city := range cities {
		measurement := results[city]
		mean := measurement.Sum / measurement.Count
		fmt.Println(
			city,
			"=",
			roundFloat(measurement.Min, 1),
			"/",
			roundFloat(mean, 1),
			"/",
			roundFloat(measurement.Max, 1),
		)
	}
}
