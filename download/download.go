package download

import (
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	pb "github.com/meateam/download-service/proto"
	ilogger "github.com/meateam/elasticsearch-logger"
	"github.com/sirupsen/logrus"
)

const (
	// PartSize is the number of bytes that a object part has, currently 5MB per part.
	PartSize = 5 << 20
)

// ErrBufferLength is the error returned by StreamReadCloser.Read when len(p) <= PartSize.
var ErrBufferLength error = fmt.Errorf("len(p) is required to be at least %d", PartSize)

// StreamReadCloser is a structure that implements io.Reader to read a object's bytes from stream.
type StreamReadCloser struct {
	stream pb.Download_DownloadClient
}

// NewStreamReadCloser returns a StreamReadCloser initialized with stream to read the object's bytes from.
func NewStreamReadCloser(stream pb.Download_DownloadClient) StreamReadCloser {
	return StreamReadCloser{stream: stream}
}

// Read implements io.Reader to read object's bytes into p,
// len(p) MUST be >= PartSize, otherwise Read wouldn't read the chunk into p,
// Read doesn't call r.stream.Recv() unless len(p) >= PartSize.
// If Read would've read the chunk into p where len(p) < PartSize,
// it would read incomplete object bytes into p and the reader would
// miss bytes from the object stream.
// Implementation does not retain p.
func (r StreamReadCloser) Read(p []byte) (n int, err error) {
	// Cannot read the whole bytes of a chunk's maximum number of bytes.
	// Do not call r.steam.Recv unless the whole chunk can be read into p,
	// otherwise the reader would miss bytes of the stream chunks.
	if int64(len(p)) < PartSize {
		return 0, fmt.Errorf("len(p) is required to be at least %d", PartSize)
	}

	chunk, err := r.stream.Recv()

	// Return even if err == io.EOF
	if err != nil {
		return 0, err
	}

	part := chunk.GetFile()
	read := 0

	// len(p) is big enough to safely use it without losing bytes.
	if len(p) >= len(part) {
		read = copy(p, part)
	}

	return read, nil
}

// Close closes the send direction of the underlying r.stream.
func (r StreamReadCloser) Close() error {
	return r.stream.CloseSend()
}

// Service is a structure used for downloading objects from S3.
type Service struct {
	s3Client *s3.S3
	logger   *logrus.Logger
}

// NewService creates a Service and returns it.
func NewService(s3Client *s3.S3, logger *logrus.Logger) *Service {
	return &Service{s3Client: s3Client, logger: logger}
}

// GetS3Client returns the internal s3 client.
func (s Service) GetS3Client() *s3.S3 {
	return s.s3Client
}

// Download is the request to download a object from S3.
// It receives a request for a object.
// Responds with a stream of the object bytes in chunks.
func (s Service) Download(req *pb.DownloadRequest, stream pb.Download_DownloadServer) error {
	// Fetch key and bucket from the request and check it's validity.
	key := req.GetKey()
	bucket := req.GetBucket()
	if key == "" {
		return fmt.Errorf("key is required")
	}

	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	// Get the object's length.
	objectDetails, err := s.s3Client.HeadObjectWithContext(
		stream.Context(),
		&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to download object %s/%s: %v", bucket, key, err)
	}

	// Calculate how many parts there are to download.
	totalParts := *objectDetails.ContentLength / PartSize
	if *objectDetails.ContentLength%PartSize > 0 {
		totalParts++
	}

	// Iterate over all of the parts, download each part and stream it to the client.
	for currentPart := int64(0); currentPart < totalParts; currentPart++ {
		// Calculate current part bytes range to download.
		rangeStart := currentPart * PartSize
		rangeEnd := rangeStart + PartSize - 1
		if rangeEnd > *objectDetails.ContentLength {
			rangeEnd = *objectDetails.ContentLength - 1
		}

		getObjectInput := &s3.GetObjectInput{
			Key:        aws.String(key),
			Bucket:     aws.String(bucket),
			PartNumber: aws.Int64(currentPart),
			Range:      aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
		}

		objectPartOutput, err := s.s3Client.GetObjectWithContext(stream.Context(), getObjectInput)

		if err != nil {
			return fmt.Errorf("failed to download object %s/%s: %v", bucket, key, err)
		}

		partBytes, err := ioutil.ReadAll(objectPartOutput.Body)
		if err != nil {
			return fmt.Errorf("failed to download part %d: %v", currentPart, err)
		}

		if err := stream.Send(&pb.DownloadResponse{File: partBytes}); err != nil {
			s.logger.WithFields(
				logrus.Fields{
					"trace.id": ilogger.ExtractTraceParent(stream.Context()),
				},
			).Errorf(err.Error())

			return err
		}
	}

	return nil
}
