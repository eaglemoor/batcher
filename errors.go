package batcher

import "errors"

var (
	ErrPanicRecover = errors.New("panic recover on handler func")
)
