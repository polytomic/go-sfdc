package bulk

import (
	"github.com/namely/go-sfdc/v3/session"
	"github.com/pkg/errors"
)

type BulkEndpoint string

const (
	V2IngestEndpoint BulkEndpoint = "/jobs/ingest"
	V2QueryEndpoint  BulkEndpoint = "/jobs/query"
)

// Resource is the structure that can be used to create bulk 2.0 jobs.
type Resource struct {
	session  session.ServiceFormatter
	endpoint BulkEndpoint
}

// NewResource creates a new bulk 2.0 REST ingestion resource.  If the session
// is nil an error will be returned.
func NewResource(endpoint BulkEndpoint, session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("bulk: session can not be nil")
	}

	err := session.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session:  session,
		endpoint: endpoint,
	}, nil
}

// CreateJob will create a new bulk 2.0 ingestion job from the options that
// where passed. The Job that is returned can be used to upload object data to
// the Salesforce org.
func (r *Resource) CreateJob(options Options) (*Job, error) {
	job := NewJob(r.endpoint, r.session)
	if err := job.create(options); err != nil {
		return nil, err
	}

	return job, nil
}

// GetJob will retrieve an existing bulk 2.0 job using the provided ID.
func (r *Resource) GetJob(id string) (*Job, error) {
	job := NewJob(r.endpoint, r.session)
	info, err := job.fetchInfo(id)
	if err != nil {
		return nil, err
	}
	job.info = info.Response

	return job, nil
}

// AllJobs will retrieve all of the bulk 2.0 jobs.
func (r *Resource) AllJobs(parameters Parameters) (*Jobs, error) {
	jobs, err := newJobs(r.session, r.endpoint, parameters)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}
