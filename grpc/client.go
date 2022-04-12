package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/eslywadan/python-go/grpc/pb"
	"google.golang.org/grpc"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	addr := "localhost:9999"
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewOutliersClient(conn)
	req := pb.OutliersRequest{
		Metrics: dummyData(),
	}

	resp, err := client.Detect(context.Background(), &req)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Outliers at %v", resp.Indices)
}

func dummyData() []*pb.Metric {
	const size = 1000
	out := make([]*pb.Metric, size)
	t := time.Date(2020, 5, 22, 14, 13, 11, 0, time.UTC)
	for i := 0; i < size; i++ {
		m := pb.Metric{
			Time: Timestamp(t),
			Name: "CPU",
			//Normally we're below 40% CPU Utilization
			Value: rand.Float64() * 40,
		}
		out[i] = &m
		t.Add(time.Second)
	}
	// Create Some outliers
	out[7].Value = 97.4
	out[113].Value = 92.1
	out[835].Value = 93.2
	out[931].Value = 98.3
	return out
}

// Timestamp converts time.Time to protobuf *Timestamp
func Timestamp(t time.Time) *pbtime.Timestamp {
	return &pbtime.Timestamp {
		Seconds: t.Unix(),
		Nanos: int32(t.Nanosecond()),
	}
}

