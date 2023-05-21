package batcher

import "errors"

var (
	ErrBatcherNotInit = errors.New("use batcher.New for create and init batcher")
	ErrPanicRecover   = errors.New("panic recover on handler func")
)
