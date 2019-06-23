package download_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pb "github.com/meateam/download-service/proto"
	"github.com/meateam/download-service/server"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// Declaring global variables.
var (
	logger     = logrus.New()
	lis        *bufconn.Listener
	s3Client   *s3.S3
	testbucket = "testbucket"
	testkey    = "test.txt"
	file       = make([]byte, 2<<20)
)

func init() {
	lis = bufconn.Listen(bufSize)

	// Disable log output.
	logger.SetOutput(ioutil.Discard)
	downloadServer := server.NewServer(logger)

	s3Client = downloadServer.GetService().GetS3Client()
	go func() {
		downloadServer.Serve(lis)
	}()

	file = make([]byte, 2<<20)
	if _, err := rand.Read(file); err != nil {
		log.Fatalf("failed to generate file, %v", err)
	}

	if err := emptyAndDeleteBucket(testbucket); err != nil {
		log.Printf("failed to emptyAndDeleteBucket, %v", err)
	}

	if _, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(testbucket),
	}); err != nil {
		log.Printf("failed to create bucket, %v", err)
	}

	uploader := s3manager.NewUploaderWithClient(s3Client)
	_, err := uploader.Upload(&s3manager.UploadInput{
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

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Create connection to server
			ctx := context.Background()
			conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
			if err != nil {
				t.Fatalf("failed to dial bufnet: %v", err)
			}
			defer conn.Close()

			t.Parallel()

			client := pb.NewDownloadClient(conn)
			stream, err := client.Download(tt.args.ctx, tt.args.req)

			// Unanticipated error - isn't related to tt.wantErr
			if err != nil {
				t.Fatalf("DownloadService.Download() error = %v, wantErr %v", err, tt.wantErr)
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

// EmptyBucket empties the Amazon S3 bucket and deletes it.
func emptyAndDeleteBucket(bucket string) error {
	log.Print("removing objects from S3 bucket : ", bucket)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}

	for {
		// Requesting for batch of objects from s3 bucket
		objects, err := s3Client.ListObjects(params)
		if err != nil {
			break
		}

		// Checks if the bucket is already empty
		if len((*objects).Contents) == 0 {
			log.Print("bucket is already empty")
			return nil
		}
		log.Print("first object in batch | ", *(objects.Contents[0].Key))

		// Creating an array of pointers of ObjectIdentifier
		objectsToDelete := make([]*s3.ObjectIdentifier, 0, 1000)
		for _, object := range (*objects).Contents {
			obj := s3.ObjectIdentifier{
				Key: object.Key,
			}
			objectsToDelete = append(objectsToDelete, &obj)
		}

		// Creating JSON payload for bulk delete
		deleteArray := s3.Delete{Objects: objectsToDelete}
		deleteParams := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &deleteArray,
		}

		// Running the Bulk delete job (limit 1000)
		_, err = s3Client.DeleteObjects(deleteParams)
		if err != nil {
			return err
		}
		if *(*objects).IsTruncated { //if there are more objects in the bucket, IsTruncated = true
			params.Marker = (*deleteParams).Delete.Objects[len((*deleteParams).Delete.Objects)-1].Key
			log.Print("requesting next batch | ", *(params.Marker))
		} else { // If all objects in the bucket have been cleaned up.
			break
		}
	}

	log.Print("Emptied S3 bucket : ", bucket)
	if _, err := s3Client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		log.Printf("failed to DeleteBucket, %v", err)
	}

	return nil
}
