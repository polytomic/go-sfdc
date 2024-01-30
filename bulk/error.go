package bulk

var _ error = (*JobRecordError)(nil)

func NewJobRecordError(e error) JobRecordError {
	return JobRecordError{e: e}
}

// JobRecordError wraps errors returned when retrieving records for a bulk job.
type JobRecordError struct {
	e error
}

func (e JobRecordError) Error() string {
	return e.e.Error()
}

func (e JobRecordError) Unwrap() error {
	return e.e
}
