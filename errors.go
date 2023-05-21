package batcher

import "errors"

var (
	ErrBatcherNotInit = errors.New("batcher: use batcher.New for create and init batcher")
	ErrPanicRecover   = errors.New("batcher: panic recover on handler func")
	ErrShotdown       = errors.New("batcher: shut down")
)
