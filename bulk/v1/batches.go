package bulkv1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"
)

// JobBatchResponse is the response structure for the Job Batches request
type JobBatchResponse struct {
	Batches []BatchInfo `json:"batchInfo"`
}

// JobBatches describes the collection of Batches associated with a Bulk v1 Job
type JobBatches struct {
	session session.ServiceFormatter
	job     bulk.Response
	info    []BatchInfo
}

func (b *JobBatches) fetchInfo(ctx context.Context) (err error) {
	url := bulkEndpoint(b.session, b.job.ID, "batch")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	b.session.AuthorizationHeader(request)

	b.info, err = b.infoResponse(request)
	return err
}

func (b *JobBatches) infoResponse(request *http.Request) ([]BatchInfo, error) {
	response, err := b.session.Client().Do(request)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, sfdc.HandleError(response)
	}

	var value JobBatchResponse
	err = decoder.Decode(&value)
	if err != nil {
		return nil, fmt.Errorf("Error decoding batches: %w", err)
	}
	return value.Batches, nil
}

// All returns a slice of individual Batches
func (b *JobBatches) All() []*Batch {
	batches := make([]*Batch, len(b.info))
	for i, info := range b.info {
		batches[i] = &Batch{
			session: b.session,
			Info:    info,
		}
	}

	return batches
}

// Create creates a new Batch in the Job
func (b *JobBatches) Create(ctx context.Context, body io.Reader) (*Batch, error) {
	batch := &Batch{
		session: b.session,
	}
	err := batch.create(ctx, b.job.ID, b.job.ContentType, body)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

// GetInfo retrieves the details of a single batch
func (b *JobBatches) GetInfo(ctx context.Context, batchID string) (*Batch, error) {
	batch := &Batch{
		session: b.session,
	}
	err := batch.fetchInfo(ctx, b.job.ID, batchID)
	if err != nil {
		return nil, err
	}
	return batch, nil
}
