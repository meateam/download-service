package main

import (
	"context"
	pb "download-service/proto"
	"io"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// Declaring global variables.
var s3Endpoint string
var newSession = session.Must(session.NewSession())
var s3Client *s3.S3
var lis *bufconn.Listener

func init() {
	// Fetch env vars
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3Endpoint = os.Getenv("S3_ENDPOINT")
	s3Token := ""

	// Configure to use S3 Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3AccessKey, s3SecretKey, s3Token),
		Endpoint:         aws.String(s3Endpoint),
		Region:           aws.String("eu-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	// Init real client.
	newSession = session.New(s3Config)
	s3Client = s3.New(newSession)

	lis = bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(10 << 20))
	server := DownloadService{s3Client: s3Client}
	pb.RegisterDownloadServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
}

func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}
func TestDownloadService_Download(t *testing.T) {
	type args struct {
		ctx    context.Context
		req    *pb.DownloadRequest
		stream pb.Download_DownloadServer
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}

	// Create connection to server
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewDownloadClient(conn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := client.Download(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadService.Download() error = %v, wantErr %v", err, tt.wantErr)
			}

			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if (err != nil) != tt.wantErr {
					t.Errorf("DownloadService.Download() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
