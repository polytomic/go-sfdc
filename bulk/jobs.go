package bulk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
)

// Parameters to query all of the bulk jobs.
//
// IsPkChunkingEnabled will filter jobs with PK chunking enabled.
//
// JobType will filter jobs based on job type.
type Parameters struct {
	IsPkChunkingEnabled bool
	JobType             JobType
}

type jobResponse struct {
	Done           bool       `json:"done"`
	Records        []Response `json:"records"`
	NextRecordsURL string     `json:"nextRecordsUrl"`
}

// Jobs presents the response from the all jobs request.
type Jobs struct {
	session  session.ServiceFormatter
	response jobResponse
}

func newJobs(ctx context.Context, session session.ServiceFormatter, endpoint BulkEndpoint, parameters Parameters) (*Jobs, error) {
	j := &Jobs{
		session: session,
	}
	url := session.DataServiceURL() + string(endpoint)
	request, err := j.request(ctx, url)
	if err != nil {
		return nil, err
	}
	q := request.URL.Query()
	q.Add("isPkChunkingEnabled", strconv.FormatBool(parameters.IsPkChunkingEnabled))
	q.Add("jobType", string(parameters.JobType))
	request.URL.RawQuery = q.Encode()

	response, err := j.do(request)
	if err != nil {
		return nil, err
	}
	j.response = response
	return j, nil
}

// Done indicates whether there are more jobs to get.
func (j *Jobs) Done() bool {
	return j.response.Done
}

// Records contains the information for each retrieved job.
func (j *Jobs) Records() []Response {
	return j.response.Records
}

// Next will retrieve the next batch of job information.
func (j *Jobs) Next(ctx context.Context) (*Jobs, error) {
	if j.Done() {
		return nil, errors.New("jobs: there is no more records")
	}
	request, err := j.request(ctx, j.response.NextRecordsURL)
	if err != nil {
		return nil, err
	}
	response, err := j.do(request)
	if err != nil {
		return nil, err
	}
	return &Jobs{
		session:  j.session,
		response: response,
	}, nil
}

func (j *Jobs) request(ctx context.Context, url string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "application/json, */*")
	j.session.AuthorizationHeader(request)
	return request, nil
}

func (j *Jobs) do(request *http.Request) (jobResponse, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return jobResponse{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return jobResponse{}, sfdc.HandleError(response)
	}

	var value jobResponse
	err = decoder.Decode(&value)
	if err != nil {
		return jobResponse{}, err
	}
	return value, nil

}
