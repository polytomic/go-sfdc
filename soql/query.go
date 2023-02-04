package soql

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
	"github.com/pkg/errors"
)

// Resource is the structure for the Salesforce
// SOQL API resource.
type Resource struct {
	session session.ServiceFormatter
}

type queryOpts struct {
	all             bool
	columnMetadata  bool
	noRecords       bool
	limitOffsetOpts *limitOffsetOpts
}

type limitOffsetOpts struct {
	start int
	limit int
	order string
}

type QueryOptsFunc func(opts *queryOpts) *queryOpts

// Query for all records which includes deleted records in the recycle bin.
func WithAll() QueryOptsFunc {
	return func(opts *queryOpts) *queryOpts {
		opts.all = true
		return opts
	}
}

// Add column metadata to the query result. The metadata describes the shape of
// the result regardless of nulls.
func WithColumnMetadata() QueryOptsFunc {
	return func(opts *queryOpts) *queryOpts {
		opts.columnMetadata = true
		return opts
	}
}

// Do not return any records. Combine with WithColumnMetadata to get just the
// column metadata.
func NoRecords() QueryOptsFunc {
	return func(opts *queryOpts) *queryOpts {
		opts.noRecords = true
		return opts
	}
}

// UseLimitOffsetPagination will perform pagination using limit and offset
// rather than the next page url provided by Salesforce.
func UseLimitOffsetPagination(start int, limit int, order string) QueryOptsFunc {
	return func(opts *queryOpts) *queryOpts {
		opts.limitOffsetOpts = &limitOffsetOpts{
			start: start,
			limit: limit,
			order: order,
		}
		return opts
	}
}

// NewResource forms the Salesforce SOQL resource. The
// session formatter is required to form the proper URLs and authorization
// header.
func NewResource(ctx context.Context, session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("soql: session can not be nil")
	}

	err := session.Refresh(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session: session,
	}, nil
}

// Query will call out to the Salesforce org for a SOQL.  The results will
// be the result of the query.
func (r *Resource) Query(ctx context.Context, querier QueryFormatter, qopts ...QueryOptsFunc) (QueryResult, error) {
	if querier == nil {
		return nil, errors.New("soql resource query: querier can not be nil")
	}
	opts := &queryOpts{}
	for _, opt := range qopts {
		opt(opts)
	}

	var columnMeta *QueryColumnMetadataResposne
	if opts.columnMetadata {
		cmreq, err := r.queryColumnMetadataRequest(ctx, querier)
		if err != nil {
			return nil, err
		}
		cmres, err := r.queryColumnMetadataResponse(cmreq)
		if err != nil {
			return nil, err
		}
		columnMeta = &cmres
	}

	if opts.noRecords {
		result := &QueryResultImpl{}
		result.columnMetadata = columnMeta
		return result, nil
	}

	if opts.limitOffsetOpts != nil {
		aggQuerier := &AggregationQueryFormatter{
			baseFormat: querier,
			orderBy:    opts.limitOffsetOpts.order,
			limit:      opts.limitOffsetOpts.limit,
			offset:     opts.limitOffsetOpts.start,
		}
		request, err := r.queryRequest(ctx, aggQuerier, opts.all)
		if err != nil {
			return nil, err
		}

		response, err := r.queryResponse(request)
		if err != nil {
			return nil, err
		}
		result, err := NewQueryOffsetLimitResult(response, r, aggQuerier, opts.all)
		if err != nil {
			return nil, err
		}
		result.QueryResultImpl.columnMetadata = columnMeta
		return result, nil
	}

	// normal next page querying
	request, err := r.queryRequest(ctx, querier, opts.all)
	if err != nil {
		return nil, err
	}

	response, err := r.queryResponse(request)
	if err != nil {
		return nil, err
	}

	result, err := NewQueryResult(response, r)
	if err != nil {
		return nil, err
	}
	result.columnMetadata = columnMeta
	return result, nil

}

func (r *Resource) next(ctx context.Context, recordURL string) (QueryResult, error) {
	queryURL := r.session.InstanceURL() + recordURL
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	r.session.AuthorizationHeader(request)

	response, err := r.queryResponse(request)
	if err != nil {
		return nil, err
	}

	result, err := NewQueryResult(response, r)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Resource) queryColumnMetadataRequest(ctx context.Context, querier QueryFormatter) (*http.Request, error) {
	query, err := NewQueryColumnMetadataRequest(r.session, querier)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.session.InstanceURL()+query.URL(), nil)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	r.session.AuthorizationHeader(request)
	return request, nil
}

func (r *Resource) queryRequest(ctx context.Context, querier QueryFormatter, all bool) (*http.Request, error) {
	query, err := NewQueryRequest(r.session, querier, all)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.session.InstanceURL()+query.URL(), nil)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	r.session.AuthorizationHeader(request)
	return request, nil
}

func (r *Resource) queryResponse(request *http.Request) (QueryResponse, error) {
	response, err := r.session.Client().Do(request)

	if err != nil {
		return QueryResponse{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return QueryResponse{}, sfdc.HandleError(response)
	}

	var resp QueryResponse
	err = decoder.Decode(&resp)
	if err != nil {
		return QueryResponse{}, err
	}

	return resp, nil
}

func (r *Resource) queryColumnMetadataResponse(request *http.Request) (QueryColumnMetadataResposne, error) {
	response, err := r.session.Client().Do(request)

	if err != nil {
		return QueryColumnMetadataResposne{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return QueryColumnMetadataResposne{}, sfdc.HandleError(response)
	}

	var resp QueryColumnMetadataResposne
	err = decoder.Decode(&resp)
	if err != nil {
		return QueryColumnMetadataResposne{}, err
	}

	return resp, nil
}
