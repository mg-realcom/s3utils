package s3utils

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Client struct {
	client *s3.Client
	region string
}

// NewClient creates a new client.
func NewClient(ctx context.Context, region string) (*Client, error) {
	// Loading configuration from ~/.aws/* or ENV
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, NewSDKError("unable to load SDK config", err)
	}

	// Creating the S3 client
	client := s3.NewFromConfig(cfg)

	return &Client{
		client: client,
		region: region,
	}, nil
}

// UploadFileBase uploads a file.
func (s *Client) UploadFileBase(ctx context.Context, bucketName string, directory string, filePath string, externalFilename string) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if directory == "" {
		return NewValidationError("directory is empty")
	}

	if filePath == "" {
		return NewValidationError("file path is empty")
	}

	if externalFilename == "" {
		return NewValidationError("external filename is empty")
	}

	objectKey := generateObjectKeyBase(directory, externalFilename)

	file, err := os.Open(filePath)
	if err != nil {
		return NewSDKError("unable to open file", err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return NewSDKError("unable to get file info", err)
	}

	if fileInfo.Size() == 0 {
		return NewValidationError("file is empty")
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return NewS3Error("unable to upload file", err)
	}

	return err
}

// UploadFileWithDateDestination uploads a file to folder with a specific date prefix.
func (s *Client) UploadFileWithDateDestination(ctx context.Context, bucketName string, directory string, filePath string, date time.Time) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if directory == "" {
		return NewValidationError("directory is empty")
	}

	if filePath == "" {
		return NewValidationError("file path is empty")
	}

	if date.IsZero() {
		return NewValidationError("date is empty")
	}

	objectKey := generateObjectKeyByDate(directory, filePath, date)

	file, err := os.Open(filePath)
	if err != nil {
		return NewSDKError("unable to open file", err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return NewSDKError("unable to get file info", err)
	}

	if fileInfo.Size() == 0 {
		return NewValidationError("file is empty")
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return NewS3Error("unable to upload file", err)
	}

	return err
}

// DeleteFolderByDate deletes all objects in a folder with a specific date prefix.
func (s *Client) DeleteFolderByDate(ctx context.Context, bucketName string, directory string, date time.Time) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if directory == "" {
		return NewValidationError("directory is empty")
	}

	if date.IsZero() {
		return NewValidationError("date is empty")
	}

	objectKey := generateFolderDestinationByDate(directory, date)

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	}

	listResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return NewS3Error("unable to list objects", err)
	}

	deleteObjects := make([]types.ObjectIdentifier, 0, len(listResp.Contents))
	for _, object := range listResp.Contents {
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: aws.String(*object.Key),
		})
	}

	if len(deleteObjects) == 0 {
		return nil
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: deleteObjects,
			Quiet:   aws.Bool(false),
		},
	}

	_, err = s.client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		return NewS3Error("unable to delete objects", err)
	}

	return nil
}

// DeleteFolder deletes all objects in a folder.
func (s *Client) DeleteFolder(ctx context.Context, bucketName string, directory string) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if directory == "" {
		return NewValidationError("directory is empty")
	}

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(directory),
	}

	listResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return NewS3Error("unable to list objects", err)
	}

	deleteObjects := make([]types.ObjectIdentifier, 0, len(listResp.Contents))
	for _, object := range listResp.Contents {
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: aws.String(*object.Key),
		})
	}

	if len(deleteObjects) == 0 {
		return nil
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: deleteObjects,
			Quiet:   aws.Bool(false),
		},
	}

	_, err = s.client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		return NewS3Error("unable to delete objects", err)
	}

	return nil
}

// DeleteObject delete object by key.
func (s *Client) DeleteObject(ctx context.Context, bucketName string, key string) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if key == "" {
		return NewValidationError("key is empty")
	}

	deleteObjectsInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    &key,
	}

	_, err := s.client.DeleteObject(ctx, deleteObjectsInput)
	if err != nil {
		return NewS3Error("unable to delete object", err)
	}

	return nil
}

// IsObjectExists checks if object exists.
func (s *Client) IsObjectExists(ctx context.Context, bucketName string, key string) (bool, error) {
	if bucketName == "" {
		return false, NewValidationError("bucket name is empty")
	}

	if key == "" {
		return false, NewValidationError("key is empty")
	}

	key = strings.Trim(key, "/")

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: &key,
	}

	listObjectsResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return false, NewS3Error("unable to list objects", err)
	}

	if len(listObjectsResp.Contents) == 0 {
		return false, nil
	}

	return true, nil
}

// GetObject downloads object.
func (s *Client) GetObject(ctx context.Context, bucketName string, key string, localPath string) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	if key == "" {
		return NewValidationError("key is empty")
	}

	if localPath == "" {
		return NewValidationError("local path is empty")
	}

	key = strings.Trim(key, "/")

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    &key,
	}

	result, err := s.client.GetObject(ctx, getObjectInput)
	if err != nil {
		return NewS3Error("unable to get object", err)
	}

	defer result.Body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return NewSDKError("unable to create file", err)
	}

	defer file.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return NewSDKError("unable to read S3 response body", err)
	}

	_, err = file.Write(body)
	if err != nil {
		return NewSDKError("unable to write file", err)
	}

	return nil
}

// CreateBucket creates bucket.
func (s *Client) CreateBucket(ctx context.Context, bucketName string) error {
	if bucketName == "" {
		return NewValidationError("bucket name is empty")
	}

	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		},
	})
	if err != nil {
		return NewS3Error("unable to create bucket", err)
	}

	return err
}

func generateObjectKeyByDate(directory string, filePath string, date time.Time) string {
	directory = strings.Trim(directory, "/")
	fileName := strings.Split(filePath, "/")[len(strings.Split(filePath, "/"))-1]
	objectKey := fmt.Sprintf("%s/_year=%v/_month=%v/_day=%v/_date=%v/%s", directory, date.Year(), date.Format("01"), date.Format("02"), date.Format(time.DateOnly), fileName)

	return objectKey
}

func generateObjectKeyBase(directory string, filename string) string {
	directory = strings.Trim(directory, "/")
	objectKey := fmt.Sprintf("%s/%s", directory, filename)

	return objectKey
}

func generateFolderDestinationByDate(directory string, date time.Time) string {
	directory = strings.Trim(directory, "/")
	objectKey := fmt.Sprintf("%s/_year=%v/_month=%v/_day=%v/_date=%v", directory, date.Year(), date.Format("01"), date.Format("02"), date.Format(time.DateOnly))

	return objectKey
}
