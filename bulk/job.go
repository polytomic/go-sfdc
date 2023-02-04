package bulk

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
)

// JobType is the bulk job type.
type JobType string

const (
	// BigObjects is the big objects job.
	BigObjects JobType = "BigObjectIngest"
	// Classic is the bulk job 1.0.
	Classic JobType = "Classic"
	// V2Ingest is the bulk job 2.0.
	V2Ingest JobType = "V2Ingest"
	V2Query  JobType = "V2Query"
)

// ColumnDelimiter is the column delimiter used for CSV job data.
type ColumnDelimiter string

const (
	// Backquote is the (`) character.
	Backquote ColumnDelimiter = "BACKQUOTE"
	// Caret is the (^) character.
	Caret ColumnDelimiter = "CARET"
	// Comma is the (,) character.
	Comma ColumnDelimiter = "COMMA"
	// Pipe is the (|) character.
	Pipe ColumnDelimiter = "PIPE"
	// SemiColon is the (;) character.
	SemiColon ColumnDelimiter = "SEMICOLON"
	// Tab is the (\t) character.
	Tab ColumnDelimiter = "TAB"
)

// ContentType is the format of the data being processed.
type ContentType string

const (
	// CSV is the supported content data type for Bulk v2 Jobs
	CSV ContentType = "CSV"
	// JSON is the supported content data type for Bulk v1 Jobs
	JSON ContentType = "JSON"
)

// LineEnding is the line ending used for the CSV job data.
type LineEnding string

const (
	// Linefeed is the (\n) character.
	Linefeed LineEnding = "LF"
	// CarriageReturnLinefeed is the (\r\n) character.
	CarriageReturnLinefeed LineEnding = "CRLF"
)

// ConcurrencyMode determines how Salesforce processes the Job batches
type ConcurrencyMode string

const (
	// Serial batches will be processed by Salesforce sequentially
	Serial ConcurrencyMode = "Serial"
	// Parallel batches will be processed by Salesforce simultaneously
	Parallel ConcurrencyMode = "Parallel"
)

// Operation is the processing operation for the job.
type Operation string

const (
	// Insert is the object operation for inserting records.
	Insert Operation = "insert"
	// Delete is the object operation for deleting records.
	Delete Operation = "delete"
	// Update is the object operation for updating records.
	Update Operation = "update"
	// Upsert is the object operation for upserting records.
	Upsert   Operation = "upsert"
	Query    Operation = "query"
	QueryAll Operation = "queryAll"
)

func (o Operation) IsValid() bool {
	switch o {
	case Insert, Delete, Update, Upsert, Query, QueryAll:
		return true
	}
	return false
}

// State is the current state of processing for the job.
type State string

const (
	// Open the job has been created and job data can be uploaded tothe job.
	Open State = "Open"
	// Closed jobs have started processing; new data may not be added
	Closed State = "Closed"
	// UpdateComplete all data for the job has been uploaded and the job is ready to be queued and processed.
	UpdateComplete State = "UploadComplete"
	// Aborted the job has been aborted.
	Aborted State = "Aborted"
	// JobComplete the job was processed by Salesforce.
	JobComplete State = "JobComplete"
	// Failed some records in the job failed.
	Failed State = "Failed"
)

const (
	// sfID is the column name for the Salesforce Object ID in Job CSV responses
	sfID = "sf__Id"

	// sfError is the column name for the error in Failed record responses
	sfError = "sf__Error"

	// sfError is the column name for the created flag in Successful record responses
	sfCreated = "sf__Created"
)

const (
	// HeaderLocator is the HTTP header which contains the locator for the next
	// page of results.
	HeaderLocator = "Sforce-Locator"
	// HeaderRecordCount is the HTTP header which contains the total number of
	// records from the query.
	HeaderRecordCount = "Sforce-NumberOfRecords"
)

// UnprocessedRecord is the unprocessed records from the job.
type UnprocessedRecord struct {
	Fields map[string]interface{}
}

// JobRecord is the record for the job.  Includes the Salesforce ID along with the fields.
type JobRecord struct {
	ID string
	UnprocessedRecord
}

// SuccessfulRecord indicates for the record was created and the data that was uploaded.
type SuccessfulRecord struct {
	Created bool
	JobRecord
}

// FailedRecord indicates why the record failed and the data of the record.
type FailedRecord struct {
	Error string
	JobRecord
}

// ResultsPage contains a page of bulk query results, along with the locator for
// the next page, if any.
type ResultsPage struct {
	Records []map[string]interface{}
	Locator string
}

// Options are the options for the job.
//
// ColumnDelimiter is the delimiter used for the CSV job.  This field is optional.
//
// ContentType is the content type for the job.  This field is optional.
//
// ExternalIDFieldName is the external ID field in the object being updated.  Only needed for
// upsert operations.  This field is required for upsert operations.
//
// LineEnding is the line ending used for the CSV job data.  This field is optional.
//
// Object is the object type for the data bneing processed. This field is required.
//
// Operation is the processing operation for the job. This field is required.
type Options struct {
	Query               string          `json:"query,omitempty"`
	ColumnDelimiter     ColumnDelimiter `json:"columnDelimiter,omitempty"`
	ContentType         ContentType     `json:"contentType,omitempty"`
	ExternalIDFieldName string          `json:"externalIdFieldName,omitempty"`
	LineEnding          LineEnding      `json:"lineEnding,omitempty"`
	Object              string          `json:"object,omitempty"`
	Operation           Operation       `json:"operation"`
}

// Response is the response to job APIs.
type Response struct {
	APIVersion          float32         `json:"apiVersion"`
	ColumnDelimiter     ColumnDelimiter `json:"columnDelimiter"`
	ConcurrencyMode     ConcurrencyMode `json:"concurrencyMode"`
	ContentType         ContentType     `json:"contentType"`
	ContentURL          string          `json:"contentUrl"`
	CreatedByID         string          `json:"createdById"`
	CreatedDate         string          `json:"createdDate"`
	ExternalIDFieldName string          `json:"externalIdFieldName"`
	ID                  string          `json:"id"`
	JobType             JobType         `json:"jobType"`
	LineEnding          LineEnding      `json:"lineEnding"`
	Object              string          `json:"object"`
	Operation           Operation       `json:"operation"`
	State               State           `json:"state"`
	SystemModstamp      string          `json:"systemModstamp"`
}

// Info is the response to the job information API.
type Info struct {
	Response
	ApexProcessingTime      int    `json:"apexProcessingTime"`
	APIActiveProcessingTime int    `json:"apiActiveProcessingTime"`
	NumberRecordsFailed     int    `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int    `json:"numberRecordsProcessed"`
	Retries                 int    `json:"retries"`
	TotalProcessingTime     int    `json:"totalProcessingTime"`
	ErrorMessage            string `json:"errorMessage"`
}

// Job is the bulk job.
type Job struct {
	endpoint BulkEndpoint
	session  session.ServiceFormatter
	info     Response
}

func NewJob(endpoint BulkEndpoint, session session.ServiceFormatter) *Job {
	return &Job{
		endpoint: endpoint,
		session:  session,
	}
}

func (j *Job) create(ctx context.Context, options Options) error {
	err := j.formatOptions(&options)
	if err != nil {
		return err
	}
	j.info, err = j.createCallout(ctx, options)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) formatOptions(options *Options) error {
	if !options.Operation.IsValid() {
		return errors.New("bulk job: invalid operation")
	}
	switch options.Operation {
	case Upsert:
		if options.ExternalIDFieldName == "" {
			return errors.New("bulk job: external id field name is required for upsert operation")
		}
	case Query, QueryAll:
		if options.Query == "" {
			return errors.New("bulk job: query is required")
		}
	}
	// Object is required for non-query operations
	if options.Query == "" && options.Object == "" {
		return errors.New("bulk job: object is required")
	}
	if options.LineEnding == "" {
		options.LineEnding = Linefeed
	}
	if options.ContentType == "" {
		options.ContentType = CSV
	}
	if options.ColumnDelimiter == "" {
		options.ColumnDelimiter = Comma
	}
	return nil
}

func (j *Job) createCallout(ctx context.Context, options Options) (Response, error) {
	url := j.session.DataServiceURL() + string(j.endpoint)
	body, err := json.Marshal(options)
	if err != nil {
		return Response{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

func (j *Job) response(request *http.Request) (Response, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return Response{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return Response{}, sfdc.HandleError(response)
	}

	var value Response
	err = decoder.Decode(&value)
	if err != nil {
		return Response{}, err
	}
	return value, nil
}

// Info returns the current job information.
func (j *Job) Info(ctx context.Context) (Info, error) {
	return j.fetchInfo(ctx, j.info.ID)
}

func (j *Job) fetchInfo(ctx context.Context, id string) (Info, error) {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + id
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Info{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.infoResponse(request)
}

func (j *Job) infoResponse(request *http.Request) (Info, error) {
	response, err := j.session.Client().Do(request)
	if err != nil {
		return Info{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return Info{}, err
	}

	decoder := json.NewDecoder(response.Body)
	var value Info
	err = decoder.Decode(&value)
	if err != nil {
		return Info{}, err
	}
	return value, nil
}

func (j *Job) setState(ctx context.Context, state State) (Response, error) {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID
	jobState := struct {
		State string `json:"state"`
	}{
		State: string(state),
	}
	body, err := json.Marshal(jobState)
	if err != nil {
		return Response{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	j.session.AuthorizationHeader(request)

	return j.response(request)
}

// Close will close the current job.
func (j *Job) Close(ctx context.Context) (Response, error) {
	return j.setState(ctx, UpdateComplete)
}

// Abort will abort the current job.
func (j *Job) Abort(ctx context.Context) (Response, error) {
	return j.setState(ctx, Aborted)
}

// Delete will delete the current job.
func (j *Job) Delete(ctx context.Context) error {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return errors.New("job error: unable to delete job")
	}
	return nil
}

// Upload will upload data to processing.
func (j *Job) Upload(ctx context.Context, body io.Reader) error {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID + "/batches"
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return sfdc.HandleError(response)
	}
	return nil
}

func (j *Job) Wait(ctx context.Context) (Info, error) {
	completed := make(chan Info)
	errs := make(chan error, 1)
	go func() {
		defer func() {
			close(completed)
			close(errs)
		}()
		bo := jobWaitBackoff()
		for {
			select {
			case <-ctx.Done():
				// abort the job if the context was cancelled before we
				// returned; use context.Background() since calling context is
				// already Done.
				j.Abort(context.Background())
				return
			case <-time.After(bo.NextBackOff()):
			}
			status, err := j.Info(ctx)
			if err != nil {
				errs <- err
				return
			}
			if status.State == JobComplete {
				completed <- status
				return
			}

		}
	}()
	select {
	case <-ctx.Done():
		return Info{}, ctx.Err()
	case info := <-completed:
		return info, nil
	case err := <-errs:
		return Info{}, err
	}
}

// Results returns a page of results from a Query job.
func (j *Job) Results(ctx context.Context, locator string, maxRecords int) (*ResultsPage, error) {
	if j.endpoint != V2QueryEndpoint {
		return nil, errors.New("job error: results only available for query jobs")
	}
	resultsURL := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID + "/results"
	params := url.Values{}
	if locator != "" {
		params.Add("locator", locator)
	}
	if maxRecords > 0 {
		params.Add("maxRecords", strconv.Itoa(maxRecords))
	}
	if len(params) > 0 {
		resultsURL += "?" + params.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, resultsURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	reader := csv.NewReader(response.Body)
	reader.LazyQuotes = true
	reader.Comma = j.delimiter()

	count, _ := strconv.Atoi(response.Header.Get(HeaderRecordCount))
	result := ResultsPage{
		Records: make([]map[string]interface{}, 0, count),
		Locator: response.Header.Get(HeaderLocator),
	}

	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &result, err
		}
		result.Records = append(result.Records, j.record(fields, values))
	}

	return &result, nil
}

// SuccessfulRecords returns the successful records for the job.
func (j *Job) SuccessfulRecords(ctx context.Context) ([]SuccessfulRecord, error) {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID + "/successfulResults/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	reader := csv.NewReader(response.Body)
	reader.LazyQuotes = true
	reader.Comma = j.delimiter()

	var records []SuccessfulRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		var record SuccessfulRecord
		created, err := strconv.ParseBool(values[j.headerPosition(sfCreated, fields)])
		if err != nil {
			return records, err
		}
		record.Created = created
		record.ID = values[j.headerPosition(sfID, fields)]
		record.Fields = j.record(fields[2:], values[2:])
		records = append(records, record)
	}

	return records, nil
}

// FailedRecords returns the failed records for the job.
func (j *Job) FailedRecords(ctx context.Context) ([]FailedRecord, error) {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID + "/failedResults/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	reader := csv.NewReader(response.Body)
	reader.LazyQuotes = true
	reader.Comma = j.delimiter()

	var records []FailedRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		var record FailedRecord
		record.Error = values[j.headerPosition(sfError, fields)]
		record.ID = values[j.headerPosition(sfID, fields)]
		record.Fields = j.record(fields[2:], values[2:])
		records = append(records, record)
	}

	return records, nil
}

// UnprocessedRecords returns the unprocessed records for the job.
func (j *Job) UnprocessedRecords(ctx context.Context) ([]UnprocessedRecord, error) {
	url := j.session.DataServiceURL() + string(j.endpoint) + "/" + j.info.ID + "/unprocessedrecords/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Accept", "text/csv")
	j.session.AuthorizationHeader(request)

	response, err := j.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	reader := csv.NewReader(response.Body)
	reader.LazyQuotes = true
	reader.Comma = j.delimiter()

	var records []UnprocessedRecord
	fields, err := reader.Read()
	if err != nil {
		return nil, err
	}
	for {
		values, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		var record UnprocessedRecord
		record.Fields = j.record(fields, values)
		records = append(records, record)
	}

	return records, nil
}

func (j *Job) headerPosition(column string, header []string) int {
	for idx, col := range header {
		if col == column {
			return idx
		}
	}
	return -1
}

func (j *Job) fields(header []string, offset int) []string {
	fields := make([]string, len(header)-offset)
	copy(fields[:], header[offset:])
	return fields
}

func (j *Job) record(fields, values []string) map[string]interface{} {
	record := make(map[string]interface{})
	for idx, field := range fields {
		record[field] = values[idx]
	}
	return record
}

func (j *Job) delimiter() rune {
	switch ColumnDelimiter(j.info.ColumnDelimiter) {
	case Tab:
		return '\t'
	case SemiColon:
		return ';'
	case Pipe:
		return '|'
	case Caret:
		return '^'
	case Backquote:
		return '`'
	default:
		return ','
	}
}

func jobWaitBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second * 30
	b.MaxInterval = time.Minute * 10
	b.MaxElapsedTime = 0
	b.Reset()

	return b
}
