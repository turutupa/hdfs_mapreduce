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
		values := make([]int, 0)
		valueStrings := strings.Split(value, delimiter)
		for _, valueString := range valueStrings {
			valueInt, _ := strconv.Atoi(valueString)
			values = append(values, valueInt)
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

func (mr *MapReduce) Map(lineNumber int, lineText string) {
	// replace all tabs with a space
	line := ""
	for _, c := range lineText {
		if string(c) == "\t" {
			line += " "
		} else {
			line += string(c)
		}
	}
	// split line of text into words and print <word, 1> pairs
	for _, word := range strings.Split(line, " ") {
		if word != "" {
			fmt.Printf(word + delimiter + strconv.Itoa(1) + "\n")
		}
	}
}

func (mr *MapReduce) Reduce(key string, values []int) {
	count := 0
	for _, value := range values {
		count += value
	}
	fmt.Printf(key + delimiter + strconv.Itoa(count) + "\n")
}
