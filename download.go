package main

import (
	pb "download-service/proto"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	chunkSize int64 = 5 << 20 // 5MB per part
)

// DownloadService is a structure used for downloading files from S3
type DownloadService struct {
	s3Client *s3.S3
}

// Download is the request to download a file from s3.
// It recieves a req for a file.
// Responds with a stream of the file bytes in chunks.
// TODO:
func (s DownloadService) Download(req *pb.DownloadRequest, stream pb.Download_DownloadServer) error {
	// Initialize downloader from client.
	downloader := s3manager.NewDownloaderWithClient(s.s3Client, func(d *s3manager.Downloader) {
		d.PartSize = chunkSize
	})

	// fetch key and bucket from the request and check it's validity.
	key := req.GetKey()
	bucket := req.GetBucket()
	if key == "" {
		return fmt.Errorf("key is required")
	}

	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	// Get the object's length.
	fileDetails, err := s.s3Client.HeadObjectWithContext(
		stream.Context(),
		&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return fmt.Errorf("failed to download file from %s/%s: %v", bucket, key, err)
	}

	// Calculate how many parts there are to download.
	totalParts := *fileDetails.ContentLength / downloader.PartSize
	if *fileDetails.ContentLength%downloader.PartSize > 0 {
		totalParts++
	}

	// Iterate over all of the parts, download each part and stream it to the client.
	for currentPart := int64(1); currentPart <= totalParts; currentPart++ {
		buf := aws.NewWriteAtBuffer(make([]byte, 0, int(downloader.PartSize)))
		_, err := downloader.DownloadWithContext(
			stream.Context(),
			buf,
			&s3.GetObjectInput{Key: aws.String(key), Bucket: aws.String(bucket), PartNumber: aws.Int64(currentPart)},
		)

		if err != nil {
			return fmt.Errorf("failed to download file from %s/%s: %v", bucket, key, err)
		}

		stream.Send(&pb.DownloadResponse{File: buf.Bytes()})
	}

	return nil
}
