package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mongodb/ftdc"
	"io/ioutil"
	"os"
	"strconv"
)

func mongolo(path string, chunkCount int) {
	file, _ := os.Open(path)
	data, _ := ioutil.ReadAll(file)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	iter := ftdc.ReadChunks(ctx, bytes.NewBuffer(data))
	counter := 0

	for iter.Next() {
		c := iter.Chunk()

		// Output all metrics
		for i, metric := range c.Metrics {
			fmt.Print(i, ";", metric.Key(), ";", metric.Values[0])
			for j := 1; j < len(metric.Values); j++ {
				fmt.Print(";", metric.Values[j])
			}
			fmt.Println("")
		}
		counter++
		if counter == chunkCount {
			break
		}
	}

}

func main() {

	args := os.Args[1:]

	ctx, _ := context.WithCancel(context.Background())
	fmt.Println("# --------------------- Reading ", args[1], " chunks from ", args[0])

	f, err := os.Open(args[0])
	if err != nil {
		fmt.Println("Error reading file.")
		panic(err)
	}
	data, err := ioutil.ReadAll(f)

	numDocs, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println("Second parameter should be an integer")
		panic(err)
	}

	var iter = ftdc.ReadMetrics(ctx, bytes.NewBuffer(data))
	counter := 0
	for iter.Next() {

		counter++

		doc := iter.Document()
		metricsMap := doc.ExportMap()

		fmt.Println("# ----------------------------------------------------------------------")
		i := 0
		for k, v := range metricsMap {
			fmt.Println("# ", i, k, "=", v)
			i++
		}
		if counter == numDocs {
			break
		}
	}

	numDocs, _ = strconv.Atoi(args[1])
	mongolo(args[0], numDocs)
}
