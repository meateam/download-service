package main

import (
	pb "download-service/proto"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// DownloadService is a structure used for downloading files from S3
type DownloadService struct {
	s3Downloader *s3manager.Downloader
}

// Download is the request to download a file from s3.
// It recieves a req for a file.
// Responds with a stream of the file bytes in chunks.
// TODO:
func (s DownloadService) Download(req *pb.DownloadRequest, stream pb.Download_DownloadServer) error {
	return nil
}
