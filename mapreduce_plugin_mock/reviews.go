package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const delimiter = "\t"

/**
* Runs either a MAP or REDUCE job. Prints results in sdtout
*
* expected args:
* 1) job: map/reduce
* 2) key
* 3) value
 */
func main() {
	args := os.Args
	if len(args) != 4 {
		fmt.Println("ERROR", "Unexpected number of args")
		os.Exit(1)
	}
	// TODO: add job name and configure number of mappers and reducers
	job := strings.ToLower(args[1])
	key := args[2]
	value := args[3]
	mapreducer := &MapReduce{}

	if job == "map" {
		intKey, _ := strconv.Atoi(key)
		mapreducer.Map(intKey, value)
	} else if job == "reduce" {
		values := make([]float64, 0)
		valueStrings := strings.Split(value, delimiter)
		for _, valueString := range valueStrings {
			f, _ := strconv.ParseFloat(valueString, 8)
			values = append(values, f)
		}
		mapreducer.Reduce(key, values)
	} else {
		fmt.Println("Job " + job + " not recognized. Expected 'MAP' or 'REDUCE'")
	}
}

type MapReduce struct {
}

func (mr *MapReduce) Job() {
	fmt.Println("Hello from MapReduceJob")
}

// asin,brand,title,url,image,rating,reviewUrl,totalReviews,price,originalPrice
func (mr *MapReduce) Map(lineNumber int, lineText string) {
	line := strings.Split(lineText, ",")
	brand := line[1]
	rating := line[5]
	_, err := strconv.Atoi(rating)
	if err != nil {
		return
	}
	fmt.Printf(brand + delimiter + rating + "\n")
}

func (mr *MapReduce) Reduce(key string, values []float64) {
	var sum float64 = 0
	for _, value := range values {
		sum += value
	}
	var rating float64 = sum / float64(len(values))
	r := fmt.Sprintf("%f", rating)
	fmt.Printf(key + delimiter + r + "\n")
}
