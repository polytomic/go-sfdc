package bulkv1

import (
	"fmt"
	"strings"

	"github.com/namely/go-sfdc/v3/session"
	"github.com/pkg/errors"
)

func bulkEndpoint(session session.ServiceFormatter, path ...string) string {
	return fmt.Sprintf(
		"%s/services/async/%d.0/%s",
		session.InstanceURL(), session.Version(), strings.Join(
			append([]string{"job"}, path...), "/",
		),
	)
}

// Resource is the structure that can be used to create bulk 2.0 jobs.
type Resource struct {
	session session.ServiceFormatter
}

// NewResource creates a new bulk 1.0 SOAP API resource.  If the session is nil
// an error will be returned.
func NewResource(session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("bulk: session can not be nil")
	}

	err := session.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session: session,
	}, nil
}

// CreateJob will create a new bulk 1.0 job from the options that where passed.
// The Job that is returned can be used to upload object data to the Salesforce org.
func (r *Resource) CreateJob(options Options) (*Job, error) {
	job := &Job{
		session: r.session,
	}
	if err := job.create(options); err != nil {
		return nil, err
	}

	return job, nil
}

// GetJob will retrieve an existing bulk 1.0 job using the provided ID.
func (r *Resource) GetJob(id string) (*Job, error) {
	job := &Job{
		session: r.session,
	}
	info, err := job.fetchInfo(id)
	if err != nil {
		return nil, err
	}
	job.info = info.Response

	return job, nil
}
