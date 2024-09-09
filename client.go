package s3utils

import (
	"context"
	"fmt"
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

func NewClient(ctx context.Context, region string) (*Client, error) {
	// Loading configuration from ~/.aws/*
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %w", err)
	}

	// Creating the S3 client
	client := s3.NewFromConfig(cfg)

	return &Client{
		client: client,
		region: region,
	}, nil
}

func (s *Client) UploadFileWithDateDestination(ctx context.Context, bucketName string, directory string, filePath string, date time.Time) error {
	objectKey := generateObjectKeyByDate(directory, filePath, date)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file, %v", err)
	}

	defer file.Close()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("unable to upload file, %v", err)
	}

	return err
}

// DeleteFolderByDate deletes all objects in a folder with a specific date prefix.
func (s *Client) DeleteFolderByDate(ctx context.Context, bucketName string, directory string, date time.Time) error {
	objectKey := generateFolderDestinationByDate(directory, date)

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	}

	listResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return fmt.Errorf("unable to list objects, %v", err)
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
			Quiet:   aws.Bool(true),
		},
	}

	_, err = s.client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		return fmt.Errorf("unable to delete objects, %v", err)
	}

	return nil
}

func (s *Client) DeleteFolder(ctx context.Context, bucketName string, directory string) error {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(directory),
	}

	listResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return fmt.Errorf("unable to list objects, %v", err)
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
			Quiet:   aws.Bool(true),
		},
	}

	_, err = s.client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		return fmt.Errorf("unable to delete objects, %v", err)
	}

	return nil
}

// DeleteObject deletes all objects in a folder with a specific date prefix.
func (s *Client) DeleteObject(ctx context.Context, bucketName string, key string) error {
	deleteObjectsInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    &key,
	}

	_, err := s.client.DeleteObject(ctx, deleteObjectsInput)
	if err != nil {
		return fmt.Errorf("unable to delete object, %v", err)
	}

	return nil
}

func (s *Client) IsObjectExists(ctx context.Context, bucketName string, key string) (bool, error) {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: &key,
	}

	listObjectsResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		return false, fmt.Errorf("unable to list objects, %v", err)
	}

	if len(listObjectsResp.Contents) > 0 {
		return true, nil
	}

	return false, nil
}

func (s *Client) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		},
	})
	if err != nil {
		return fmt.Errorf("unable to create bucket, %v", err)
	}

	return err
}

func generateObjectKeyByDate(directory string, filePath string, date time.Time) string {
	fileName := strings.Split(filePath, "/")[len(strings.Split(filePath, "/"))-1]
	objectKey := fmt.Sprintf("%s/_year=%v/_month=%v/_day=%v/_date=%v/%s", directory, date.Year(), date.Format("01"), date.Format("02"), date.Format(time.DateOnly), fileName)

	return objectKey
}

func generateFolderDestinationByDate(directory string, date time.Time) string {
	objectKey := fmt.Sprintf("%s/_year=%v/_month=%v/_day=%v/_date=%v", directory, date.Year(), date.Format("01"), date.Format("02"), date.Format(time.DateOnly))

	return objectKey
}
