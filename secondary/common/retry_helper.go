package common

import (
	"time"
)

// Helper object to execute a function with retries and exponential backoff
type RetryHelper struct {
	interval            time.Duration // backoff interval
	retries, maxRetries int
	call                retryFunc
	factor              int // backoff factor
}

// retryAttempt param can be used used to print log message during every attempt
type retryFunc func(retryAttempt int, lastErr error) error

func NewRetryHelper(
	maxRetries int,
	interval time.Duration,
	factor int,
	call retryFunc) *RetryHelper {

	return &RetryHelper{
		interval:   interval,
		maxRetries: maxRetries,
		factor:     factor,
		call:       call,
	}
}

func (r *RetryHelper) Run() error {
	var err error

	for ; r.retries < r.maxRetries+1; r.retries++ {
		err = r.call(r.retries, err)
		if err == nil {
			break
		} else {
			time.Sleep(r.interval)
			r.interval = r.interval * time.Duration(r.factor)
		}
	}

	return err
}

// Retry until no error or cond(err) returns true
func (r *RetryHelper) RunWithConditionalError(cond func(error) bool) error {
	var err error

	for ; r.retries < r.maxRetries+1; r.retries++ {
		err = r.call(r.retries, err)
		if err == nil || cond(err) {
			break
		} else {
			time.Sleep(r.interval)
			r.interval = r.interval * time.Duration(r.factor)
		}
	}

	return err
}
