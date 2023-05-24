package main

import (
	"context"
	"log"

	"github.com/eaglemoor/batcher"
	"github.com/eaglemoor/batcher/example/a/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	serverAHost = "localhost:8082"
)

// initBatcher make batcher with grpc call to exampleA service
func initBatcher(client pb.ExampleAClient) *batcher.Batcher[int32, *pb.Person] {
	batchFunc := func(ctx context.Context, keys []int32) []*batcher.Result[int32, *pb.Person] {
		// grpc call
		data, err := client.PersonByIDs(ctx, &pb.PersonRequest{
			Id: keys,
		})

		if err != nil {
			// if grpc return err - make response for all keys with error
			resp := batcher.FailResult[int32, *pb.Person](keys, err)
			return resp
		}

		resp := make([]*batcher.Result[int32, *pb.Person], 0, len(data.People))
		for _, item := range data.People {
			resp = append(resp, &batcher.Result[int32, *pb.Person]{
				Key:   item.Id,
				Value: item,
			})
		}

		return resp
	}

	return batcher.New(batchFunc)
}

func initClientExampleA() (pb.ExampleAClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.DialContext(context.Background(), serverAHost, opts...)
	if err != nil {
		log.Fatalf("can't init grpc client to a server: %s", err.Error())
	}

	client := pb.NewExampleAClient(conn)
	return client, conn
}

func main() {
	client, conn := initClientExampleA()
	defer conn.Close()

	b := initBatcher(client)

	ctx := context.Background()

	item, err := b.Load(ctx, 10)
	log.Printf("user #%d: %v, error: %s", 10, item, status.Convert(err).Message())

	users, errs := b.LoadMany(ctx, 10, 20, 30, 40)
	log.Printf("users: \n %v\nerrors: %v", users, errs)
}
