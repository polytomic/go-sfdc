package soql

import (
	"errors"
)

var (
	ErrNoMoreResults = errors.New("soql query result: no more records to query")
)

type QueryResult interface {
	Done() bool
	TotalSize() int
	MoreRecords() bool
	Records() []*QueryRecord
	Next() (QueryResult, error)
	ColumnMetadata() *QueryColumnMetadataResposne
	Resource() *Resource
}

// QueryResult is returned from the SOQL query.  This will
// allow for retrieving all of the records and query the
// next round of records if available.
type QueryResultImpl struct {
	response       QueryResponse
	records        []*QueryRecord
	resource       *Resource
	columnMetadata *QueryColumnMetadataResposne
}

func NewQueryResult(response QueryResponse, resource *Resource) (*QueryResultImpl, error) {
	result := &QueryResultImpl{
		response: response,
		records:  make([]*QueryRecord, len(response.Records)),
		resource: resource,
	}

	for idx, record := range response.Records {
		qr, err := newQueryRecord(record, resource)
		if err != nil {
			return nil, err
		}
		result.records[idx] = qr
	}
	return result, nil
}

// Done will indicate if the result does not contain any more records.
func (result *QueryResultImpl) Done() bool {
	return result.response.Done
}

// TotalSize is the total size of the query result.  This may
// or may not be the size of the records in the result.
func (result *QueryResultImpl) TotalSize() int {
	return result.response.TotalSize
}

// MoreRecords will indicate if the remaining records require another
// Saleforce service callout.
func (result *QueryResultImpl) MoreRecords() bool {
	return result.response.NextRecordsURL != "" && result.resource != nil
}

// Records returns the records from the query request.
func (result *QueryResultImpl) Records() []*QueryRecord {
	return result.records
}

// Next will query the next set of records.
func (result *QueryResultImpl) Next() (QueryResult, error) {
	if !result.MoreRecords() {
		return nil, ErrNoMoreResults
	}
	return result.resource.next(result.response.NextRecordsURL)
}

func (result *QueryResultImpl) ColumnMetadata() *QueryColumnMetadataResposne {
	return result.columnMetadata
}

func (result *QueryResultImpl) Resource() *Resource {
	return result.resource
}

type QueryOffsetLimitResult struct {
	*QueryResultImpl
	all   bool
	query *AggregationQueryFormatter
}

func NewQueryOffsetLimitResult(response QueryResponse, resource *Resource, query *AggregationQueryFormatter, all bool) (*QueryOffsetLimitResult, error) {
	result, err := NewQueryResult(response, resource)
	if err != nil {
		return nil, err
	}
	return &QueryOffsetLimitResult{
		QueryResultImpl: result,
		query:           query,
		all:             all,
	}, nil
}

// MoreRecords will indicate if the remaining records require another
// Saleforce service callout.
func (result *QueryOffsetLimitResult) MoreRecords() bool {
	return result.TotalSize() == result.query.limit
}

func (result *QueryOffsetLimitResult) Next() (QueryResult, error) {
	if !result.MoreRecords() {
		return nil, ErrNoMoreResults
	}
	opts := []QueryOptsFunc{
		UseLimitOffsetPagination(result.query.offset+result.query.limit, result.query.limit, result.query.orderBy),
	}
	if result.all {
		opts = append(opts, WithAll())
	}

	return result.Resource().Query(result.query.baseFormat, opts...)
}
