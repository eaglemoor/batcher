package main

import (
	context "context"
	"fmt"
	"log"
	"net"

	"github.com/eaglemoor/batcher/example/a/pb"
	"google.golang.org/grpc"
)

const (
	serverAHost = "localhost:8082"
)

func main() {
	lis, err := net.Listen("tcp", serverAHost)
	if err != nil {
		log.Fatalln(err)
	}

	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)
	pb.RegisterExampleAServer(server, &myserver{})
	server.Serve(lis)
}

type myserver struct {
	pb.ExampleAServer
}

func (s *myserver) PersonByIDs(ctx context.Context, in *pb.PersonRequest) (*pb.PersonResponse, error) {
	resp := make([]*pb.Person, 0, len(in.Id))
	for _, key := range in.Id {
		resp = append(resp, randomUser(key))
	}

	return &pb.PersonResponse{People: resp}, nil
}

func randomUser(key int32) *pb.Person {
	return &pb.Person{
		Id:   key,
		Name: fmt.Sprintf("user #%d", key),
	}
}
