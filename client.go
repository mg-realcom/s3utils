package s3utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog"
)

type Client struct {
	client *s3.Client
	region string
	logger *zerolog.Logger
}

func NewClient(ctx context.Context, logger *zerolog.Logger, region string) (*Client, error) {
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
		logger: logger,
	}, nil
}

func (s *Client) ListBuckets() {
	// Getting the list of buckets
	result, err := s.client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		s.logger.Fatal().Err(err).Msg("failed to list buckets")
	}

	for _, bucket := range result.Buckets {
		s.logger.Info().Msgf("bucket=%s creation time=%s", aws.ToString(bucket.Name), bucket.CreationDate.Format("2006-01-02 15:04:05 Monday"))
	}
}

func (s *Client) UploadFile(ctx context.Context, bucketName, objectKey, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		s.logger.Err(err).Msgf("Couldn't open file %v to upload. Here's why: %v\n", fileName, err)
	} else {
		defer file.Close()

		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			s.logger.Err(err).Msgf("Couldn't upload file %v to %v:%v. Here's why: %v\n",
				fileName, bucketName, objectKey, err)
		}
	}

	return err
}

func (s *Client) UploadFileWithDateDestination(ctx context.Context, bucketName string, directory string, filePath string, date time.Time) error {
	objectKey := generateObjectKeyByDate(directory, filePath, date)

	file, err := os.Open(filePath)
	if err != nil {
		s.logger.Err(err).Msgf("Couldn't open file %v to upload. Here's why: %v\n", filePath, err)
	} else {
		defer file.Close()

		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			s.logger.Err(err).Msgf("Couldn't upload file %v to %v:%v. Here's why: %v\n",
				filePath, bucketName, objectKey, err)
		}
	}

	return err
}

func (s *Client) DeleteFolderByDate(ctx context.Context, bucketName string, directory string, date time.Time) error {
	objectKey := generateFolderDestinationByDate(directory, date)

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	}

	listResp, err := s.client.ListObjectsV2(ctx, listObjectsInput)
	if err != nil {
		log.Fatalf("Unable to list objects in folder, %v", err)
	}

	var deleteObjects []types.ObjectIdentifier
	for _, object := range listResp.Contents {
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: aws.String(*object.Key),
		})
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

func (s *Client) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		},
	})
	if err != nil {
		s.logger.Err(err).Msgf("Couldn't create bucket %v in Region %v. Here's why: %v\n",
			bucketName, s.region, err)
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
