package s3utils

import "fmt"

type SDKError struct {
	Msg string
	Err error
}

func NewSDKError(msg string, err error) SDKError {
	return SDKError{
		Msg: msg,
		Err: err,
	}
}

func (e SDKError) Error() string {
	return fmt.Sprintf("sdk error. msg: %s. err: %v.", e.Msg, e.Err)
}

func (e SDKError) Unwrap() error {
	return e.Err
}

type ValidationError struct {
	Msg string
}

func NewValidationError(msg string) ValidationError {
	return ValidationError{
		Msg: msg,
	}
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s", e.Msg)
}

type S3Error struct {
	Msg string
	Err error
}

func NewS3Error(msg string, err error) S3Error {
	return S3Error{
		Msg: msg,
		Err: err,
	}
}

func (e S3Error) Error() string {
	return fmt.Sprintf("S3 error. msg: %s. err: %v.", e.Msg, e.Err)
}

func (e S3Error) Unwrap() error {
	return e.Err
}
