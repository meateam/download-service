package main

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pb "github.com/meateam/download-service/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// Declaring global variables.
var s3Endpoint string
var s3Client *s3.S3
var lis *bufconn.Listener
var testbucket = "testbucket"
var testkey = "test.txt"
var file = make([]byte, 2<<20)

func init() {
	// Wait until minio is up - delete it when stop using compose and start CI.
	time.Sleep(2 * time.Second)

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
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		logger.Fatalf(err.Error())
	}
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

	file = make([]byte, 2<<20)
	if _, err := rand.Read(file); err != nil {
		log.Fatalf("failed to generate file, %v", err)
	}

	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(testbucket),
	}); err != nil {
		log.Fatalf("failed to create bucket, %v", err)
	}

	uploader := s3manager.NewUploaderWithClient(s3Client)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(testbucket),
		Key:    aws.String(testkey),
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		log.Fatalf("failed to upload file, %v", err)
	}
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestDownloadService_Download(t *testing.T) {
	type args struct {
		ctx context.Context
		req *pb.DownloadRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    []byte
	}{
		{
			name: "download",
			args: args{
				ctx: context.Background(),
				req: &pb.DownloadRequest{
					Key:    testkey,
					Bucket: testbucket,
				},
			},
			wantErr: false,
			want:    file,
		},
		{
			name: "download - key does not exist",
			args: args{
				ctx: context.Background(),
				req: &pb.DownloadRequest{
					Key:    "testkey",
					Bucket: testbucket,
				},
			},
			wantErr: true,
		},
		{
			name: "download - bucket does not exist",
			args: args{
				ctx: context.Background(),
				req: &pb.DownloadRequest{
					Key:    testkey,
					Bucket: "testbucket",
				},
			},
			wantErr: true,
		},
		{
			name: "download - key is nil",
			args: args{
				ctx: context.Background(),
				req: &pb.DownloadRequest{
					Bucket: testbucket,
				},
			},
			wantErr: true,
		},
		{
			name: "download - bucket is nil",
			args: args{
				ctx: context.Background(),
				req: &pb.DownloadRequest{
					Key: testkey,
				},
			},
			wantErr: true,
		},
	}

	// Create connection to server
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewDownloadClient(conn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := client.Download(tt.args.ctx, tt.args.req)

			// unanticipated error - isn't related to tt.wantErr
			if err != nil {
				t.Errorf("DownloadService.Download() error = %v, wantErr %v", err, tt.wantErr)
			}

			fileFromStream := make([]byte, 0, 2<<20)

			for {
				ret, err := stream.Recv()
				if err == io.EOF && tt.wantErr == false {
					break
				}
				if (err != nil) && (tt.wantErr == true) {
					break
				}
				if (err != nil) && (tt.wantErr == false) {
					t.Errorf("DownloadService.Download() error = %v, wantErr %v", err, tt.wantErr)
				}

				fileFromStream = append(fileFromStream, ret.GetFile()...)
			}

			if !bytes.Equal(fileFromStream, tt.want) && tt.wantErr == false {
				t.Errorf(
					"DownloadService.Download() file downloaded is different from the wanted file, wantErr %v",
					tt.wantErr,
				)
			}
		})
	}
}
