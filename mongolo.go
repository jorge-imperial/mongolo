package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/mongodb/ftdc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

func dumpMetricsToScreen(path string, start int, end int) {
	file, _ := os.Open(path)
	data, _ := ioutil.ReadAll(file)
	file.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	iter := ftdc.ReadChunks(ctx, bytes.NewBuffer(data))
	counter := 0

	for iter.Next() {
		c := iter.Chunk()

		counter++
		if counter >= start {
			// Output all metrics
			for i, metric := range c.Metrics {
				fmt.Print(i, ";", metric.Key(), ";", metric.Values[0])
				for j := 1; j < len(metric.Values); j++ {
					fmt.Print(";", metric.Values[j])
				}
				fmt.Println("")
			}
		}

		if counter == end {
			break
		}
	}
}

func dumpNamesToScreen(fileName string, start int, end int) {
	ctx, _ := context.WithCancel(context.Background())
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error reading file.")
		panic(err)
	}
	data, err := ioutil.ReadAll(f)

	var iter = ftdc.ReadMetrics(ctx, bytes.NewBuffer(data))
	counter := 0
	for iter.Next() {
		counter++
		doc := iter.Document()
		metricsMap := doc.ExportMap()

		if counter >= start {
			i := 0
			for k, v := range metricsMap {
				fmt.Println("# ", i, k, "=", v)
				i++
			}
			fmt.Println("# ----------------------------------------------------------------------")
		}

		if counter == end {
			break
		}
	}
	f.Close()
}

func dumpMetricsToCollections(fileName string, uri string, start int, end int) {

	// "mongodb+srv://<username>:<password>@lighthouse.rmm5w.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI(uri).
		SetServerAPIOptions(serverAPIOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	database := client.Database(strings.Replace(fileName, ".", "_", -1))

	// Read all in.
	file, _ := os.Open(fileName)
	data, _ := ioutil.ReadAll(file)
	file.Close()
	defer cancel()

	iter := ftdc.ReadChunks(ctx, bytes.NewBuffer(data))
	counter := 0

	for iter.Next() {
		c := iter.Chunk()
		m := c.GetMetadata().ExportMap()
		fmt.Println(m["_id"])
		doc := m["doc"]

		fmt.Println(doc)
		counter++
		if counter >= start {
			// First metric is timestamp
			ts := c.Metrics[0].Values
			for i, metric := range c.Metrics {
				fmt.Println(i, metric.Key())

				for j := 0; j < len(metric.Values); j++ {
					//
					collName := strings.Replace(metric.Key(), ".", "_", -1)
					coll := database.Collection(collName)
					doc := bson.D{{"ts", ts[j]}, {"value", metric.Values[j]}}
					fmt.Println(coll, doc)
					_, err := coll.InsertOne(context.TODO(), doc)
					if err != nil {
						panic(err)
					}

				}
			}
		}

		if counter == end {
			break
		}
	}

}

func main() {

	// Command line variables
	var startChunk int
	var endChunk int
	var dumpNames bool
	var dumpMetrics bool
	var dumpMongo bool
	var fileName string
	var connectionString string
	flag.IntVar(&startChunk, "start", 0, "Starting chunk to dump (zero is first chunk).")
	flag.IntVar(&endChunk, "end", 0, "Last chunk to dump.")
	flag.BoolVar(&dumpNames, "names", true, "Output metric names contents to stdout.")
	flag.BoolVar(&dumpMetrics, "metrics", false, "Output metric values to stdout.")
	flag.BoolVar(&dumpMongo, "mongo", false, "Create collections using metrics.")

	flag.StringVar(&fileName, "file", "", "Path to file to read.")
	flag.StringVar(&connectionString, "uri", "mongodb://localhost:27017/", "Connection string to MongoDB database")

	flag.Parse()

	// make sure we have sane start and stop chunk values
	if endChunk < startChunk {
		endChunk = startChunk
	}

	fmt.Println("# Reading ", fileName, " chunks from ", startChunk, " to ", endChunk)

	if dumpNames {
		dumpNamesToScreen(fileName, startChunk, endChunk)
	}
	if dumpMetrics {
		dumpMetricsToScreen(fileName, startChunk, endChunk)
	}
	if dumpMongo {
		dumpMetricsToCollections(fileName, connectionString, startChunk, endChunk)
	}
}
