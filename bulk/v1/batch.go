package bulkv1

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"
)

// batchContentTypes maps the Bulk Job content type to the batch upload
// content-type header
var batchContentTypes = map[bulk.ContentType]string{
	bulk.CSV:  "text/csv",
	bulk.JSON: "application/json",
}

// BatchResult contains the result records for a completed batch
type BatchResult struct {
	Successful []bulk.SuccessfulRecord
	Failed     []bulk.FailedRecord
}

// BatchState is the current state of an individual batch
type BatchState string

const (
	// Queued batches have not started processing yet
	Queued BatchState = "Queued"
	// InProgress batches have started processing; if the job associated with
	// the batch is aborted, the batch is still processed to completion.
	InProgress BatchState = "InProgress"
	// Completed batches have been processed completely, and the results are
	// available.
	Completed BatchState = "Completed"
	// BatchFailed batches were unable to process the full request due to an
	// unexpected error. The StateMessage may contain more details about the
	// failure.
	BatchFailed BatchState = "Failed"
	// NotProcessed indicates the batch will not be processed.
	NotProcessed BatchState = "NotProcessed"
)

// BatchInfo is the response to the batch information API
type BatchInfo struct {
	ApexProcessingTime      int        `json:"apexProcessingTime"`
	APIActiveProcessingTime int        `json:"apiActiveProcessingTime"`
	CreatedDate             string     `json:"createdDate"`
	ID                      string     `json:"id"`
	JobID                   string     `json:"jobID"`
	NumberRecordsFailed     int        `json:"numberRecordsFailed"`
	NumberRecordsProcessed  int        `json:"numberRecordsProcessed"`
	State                   BatchState `json:"state"`
	StateMessage            string     `json:"stateMessage"`
	SystemModstamp          string     `json:"systemModstamp"`
	TotalProcessingTime     int        `json:"totalProcessingTime"`
}

// RecordError provides details about the error in the event of a
// record failure
type RecordError struct {
	Message             string      `json:"message"`
	Fields              []string    `json:"fields"`
	StatusCode          string      `json:"statusCode"`
	ExtendedErrorDetail interface{} `json:"extendedErrorDetail"`
}

// ResultRecord provides status information about an individual record
// in the batch
type ResultRecord struct {
	ID      string        `json:"id"`
	Success bool          `json:"success"`
	Created bool          `json:"created"`
	Errors  []RecordError `json:"errors"`
}

// Batch is a single batch in a Bulk v1 Job.
type Batch struct {
	session session.ServiceFormatter
	Info    BatchInfo
}

func (b *Batch) create(jobID string, contentType bulk.ContentType, body io.Reader) error {
	url := bulkEndpoint(b.session, jobID, "batch")
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", batchContentTypes[contentType])
	b.session.AuthorizationHeader(request)

	response, err := b.session.Client().Do(request)
	if err != nil {
		return err
	}

	b.Info, err = b.response(response)
	return err
}

func (b *Batch) response(response *http.Response) (BatchInfo, error) {
	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		err := sfdc.HandleError(response)
		return BatchInfo{}, err
	}

	var value BatchInfo
	err := decoder.Decode(&value)
	if err != nil {
		return BatchInfo{}, err
	}
	return value, nil
}

func (b *Batch) fetchInfo(jobID, ID string) error {
	url := bulkEndpoint(b.session, jobID, "batch", ID)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	b.session.AuthorizationHeader(request)

	b.Info, err = b.infoResponse(request)
	return err
}

func (b *Batch) infoResponse(request *http.Request) (BatchInfo, error) {
	response, err := b.session.Client().Do(request)
	if err != nil {
		return BatchInfo{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err := sfdc.HandleError(response)
		return BatchInfo{}, err
	}

	var value BatchInfo
	err = decoder.Decode(&value)
	if err != nil {
		return BatchInfo{}, err
	}
	return value, nil
}

// requestRecords retrieves the record payloads initially passed to
// the batch at the time of creation.
func (b *Batch) requestRecords() ([]map[string]interface{}, error) {
	url := bulkEndpoint(b.session, b.Info.JobID, "batch", b.Info.ID, "request")
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	b.session.AuthorizationHeader(request)

	response, err := b.session.Client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	decoder := json.NewDecoder(response.Body)
	result := []map[string]interface{}{}
	err = decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Results fetches the batch results from Salesforce
func (b *Batch) Results() (BatchResult, error) {
	result := BatchResult{}
	url := bulkEndpoint(b.session, b.Info.JobID, "batch", b.Info.ID, "result")
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return result, err
	}
	b.session.AuthorizationHeader(request)

	response, err := b.session.Client().Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, sfdc.HandleError(response)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return result, err
	}

	records := []ResultRecord{}

	err = json.Unmarshal(body, &records)
	if err != nil {
		fmt.Println()
		fmt.Println(string(body))
		return result, err
	}
	var requestRecords []map[string]interface{}
	for i, record := range records {
		if record.Success {
			result.Successful = append(result.Successful,
				bulk.SuccessfulRecord{
					Created: record.Created,
					JobRecord: bulk.JobRecord{
						ID: record.ID,
					},
				},
			)
		} else {
			if requestRecords == nil {
				requestRecords, err = b.requestRecords()
				if err != nil {
					return result, fmt.Errorf("error retrieving request: %w", err)
				}
			}
			messages := make([]string, len(record.Errors))
			for i, e := range record.Errors {
				messages[i] = fmt.Sprintf("%s (%s)", e.Message, e.StatusCode)
			}

			fields := map[string]interface{}{}
			if i < len(requestRecords) {
				fields = requestRecords[i]
			}
			result.Failed = append(result.Failed,
				bulk.FailedRecord{
					Error: strings.Join(messages, "\n"),
					JobRecord: bulk.JobRecord{
						ID: record.ID,
						UnprocessedRecord: bulk.UnprocessedRecord{
							Fields: fields,
						},
					},
				},
			)
		}
	}

	return result, nil
}