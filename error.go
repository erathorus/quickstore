package quickstore

import (
	"fmt"
)

const (
	ErrCodeSerializeException = "SerializeException"
	ErrCodeItemExisted        = "ItemExisted"
	ErrCodeItemNotExisted     = "ItemNotExisted"
	ErrCodeTooManyRequests    = "TooManyRequests"
	ErrCodeDynamoDBException  = "DynamoDBException"
)

type ErrSerializeException struct {
	baseErr
}

func newErrSerializeException(message string, cause error) *ErrSerializeException {
	return &ErrSerializeException{
		baseErr: baseErr{
			code:    ErrCodeSerializeException,
			message: message,
			cause:   cause,
		},
	}
}

type ErrItemExisted struct {
	baseErr
	key Key
}

func newErrItemExisted(key Key) *ErrItemExisted {
	return &ErrItemExisted{
		baseErr: baseErr{
			code:    ErrCodeItemExisted,
			message: fmt.Sprintf("item with key %v is already existed", key),
		},
		key: key,
	}
}

func (e *ErrItemExisted) Key() Key {
	return e.key
}

type ErrItemNotExisted struct {
	baseErr
	key Key
}

func newErrItemNotExisted(key Key) *ErrItemNotExisted {
	return &ErrItemNotExisted{
		baseErr: baseErr{
			code:    ErrCodeItemNotExisted,
			message: fmt.Sprintf("item with key %v is not existed", key),
		},
		key: key,
	}
}

type ErrDynamoDBException struct {
	baseErr
}

func newErrDynamoDBException(cause error) *ErrDynamoDBException {
	return &ErrDynamoDBException{
		baseErr: baseErr{
			code:    ErrCodeDynamoDBException,
			message: "error from DynamoDB",
			cause:   cause,
		},
	}
}

type ErrTooManyRequests struct {
	baseErr
}

func newErrTooManyRequests(message string) *ErrTooManyRequests {
	return &ErrTooManyRequests{
		baseErr: baseErr{
			code:    ErrCodeTooManyRequests,
			message: message,
		},
	}
}

type Error interface {
	error
	Code() string
	Message() string
	Cause() error
}

type baseErr struct {
	code    string
	message string
	cause   error
}

func (b baseErr) Error() string {
	msg := fmt.Sprintf("%s: %s", b.code, b.message)
	if b.cause != nil {
		msg = fmt.Sprintf("%s\ncaused by: %s", msg, b.cause.Error())
	}
	return msg
}

func (b baseErr) String() string {
	return b.Error()
}

func (b baseErr) Code() string {
	return b.code
}

func (b baseErr) Message() string {
	return b.message
}

func (b baseErr) Cause() error {
	return b.cause
}
