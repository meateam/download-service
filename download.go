package main

import (
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	pb "github.com/meateam/download-service/proto"
)

const (
	partSize int64 = 5 << 20 // 5MB per part
)

// DownloadService is a structure used for downloading files from S3
type DownloadService struct {
	s3Client *s3.S3
}

// Download is the request to download a file from s3.
// It receives a req for a file.
// Responds with a stream of the file bytes in chunks.
func (s DownloadService) Download(req *pb.DownloadRequest, stream pb.Download_DownloadServer) error {
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
	totalParts := *fileDetails.ContentLength / partSize
	if *fileDetails.ContentLength%partSize > 0 {
		totalParts++
	}

	// Iterate over all of the parts, download each part and stream it to the client.
	for currentPart := int64(0); currentPart < totalParts; currentPart++ {
		rangeStart := currentPart * partSize
		rangeEnd := rangeStart + partSize - 1
		if rangeEnd > *fileDetails.ContentLength {
			rangeEnd = *fileDetails.ContentLength - 1
		}

		getObjectInput := &s3.GetObjectInput{
			Key:        aws.String(key),
			Bucket:     aws.String(bucket),
			PartNumber: aws.Int64(currentPart),
			Range:      aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
		}

		objectPartOutput, err := s.s3Client.GetObjectWithContext(stream.Context(), getObjectInput)

		if err != nil {
			return fmt.Errorf("failed to download file from %s/%s: %v", bucket, key, err)
		}

		partBytes, err := ioutil.ReadAll(objectPartOutput.Body)
		if err != nil {
			return fmt.Errorf("failed to download part %d: %v", currentPart, err)
		}

		stream.Send(&pb.DownloadResponse{File: partBytes})
	}

	return nil
}
