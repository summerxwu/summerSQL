package util

import "fmt"

const (
	BatchSizeExhausted = 101
)

type Error struct {
	msg    string
	number int
}

func NewErrorf(format string, args ...any) error {
	return &Error{msg: fmt.Sprintf(format, args...), number: 0}
}

func NewErrorfWithCode(code int, format string, args ...any) error {
	return &Error{msg: fmt.Sprintf(format, args...), number: code}
}

func (e *Error) Error() string {
	return e.msg
}

func (e *Error) Code() int {
	return e.number
}
