package bulkv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"
)

// Options are the options for the job.
//
// ExternalIDFieldName is the external ID field in the object being updated.  Only needed for
// upsert operations.  This field is required for upsert operations.
//
// Object is the object type for the data bneing processed. This field is required.
//
// Operation is the processing operation for the job. This field is required.
type Options struct {
	ExternalIDFieldName string               `json:"externalIdFieldName"`
	ContentType         bulk.ContentType     `json:"contentType"`
	Object              string               `json:"object"`
	Operation           bulk.Operation       `json:"operation"`
	ConcurrencyMode     bulk.ConcurrencyMode `json:"concurrencyMode"`
}

// Job is the bulk job.
type Job struct {
	session session.ServiceFormatter
	info    bulk.Response
}

func (j *Job) create(options Options) error {
	err := j.formatOptions(&options)
	if err != nil {
		return err
	}
	j.info, err = j.createCallout(options)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) formatOptions(options *Options) error {
	if options.Operation == "" {
		return errors.New("bulk job: operation is required")
	}
	if options.Operation == bulk.Upsert {
		if options.ExternalIDFieldName == "" {
			return errors.New("bulk job: external id field name is required for upsert operation")
		}
	}
	if options.Object == "" {
		return errors.New("bulk job: object is required")
	}
	if options.ContentType == "" {
		options.ContentType = bulk.JSON
	}
	if options.ConcurrencyMode == "" {
		options.ConcurrencyMode = bulk.Parallel
	}

	return nil
}

func (j *Job) createCallout(options Options) (bulk.Response, error) {
	url := bulkEndpoint(j.session)
	body, err := json.Marshal(options)
	if err != nil {
		return bulk.Response{}, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return bulk.Response{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

func (j *Job) response(request *http.Request) (bulk.Response, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return bulk.Response{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		err = sfdc.HandleError(response)
		return bulk.Response{}, err
	}

	var value bulk.Response
	err = decoder.Decode(&value)
	if err != nil {
		return bulk.Response{}, err
	}
	return value, nil
}

// Info returns the current job information.
func (j *Job) Info() (bulk.Info, error) {
	return j.fetchInfo(j.info.ID)
}

func (j *Job) fetchInfo(id string) (bulk.Info, error) {
	url := bulkEndpoint(j.session, id)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return bulk.Info{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.infoResponse(request)
}

func (j *Job) infoResponse(request *http.Request) (bulk.Info, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return bulk.Info{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return bulk.Info{}, sfdc.HandleError(response)
	}

	var value bulk.Info
	err = decoder.Decode(&value)
	if err != nil {
		return bulk.Info{}, err
	}
	return value, nil
}

func (j *Job) setState(state bulk.State) (bulk.Response, error) {
	url := bulkEndpoint(j.session, j.info.ID)
	jobState := struct {
		State string `json:"state"`
	}{
		State: string(state),
	}
	body, err := json.Marshal(jobState)
	if err != nil {
		return bulk.Response{}, err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return bulk.Response{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

// Close will close the current job.
func (j *Job) Close() (bulk.Response, error) {
	return j.setState(bulk.Closed)
}

// Abort will abort the current job.
func (j *Job) Abort() (bulk.Response, error) {
	return j.setState(bulk.Aborted)
}

// Batches returns the collection of Batches for a Job
func (j *Job) Batches() (*JobBatches, error) {
	batches := &JobBatches{
		session: j.session,
		job:     j.info,
	}
	err := batches.fetchInfo()
	return batches, err
}
