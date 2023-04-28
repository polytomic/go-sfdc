package soql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/namely/go-sfdc/v3/composite/batch"
	"github.com/namely/go-sfdc/v3/session"
)

var (
	_ batch.Subrequester = (*QueryRequest)(nil)
)

// QueryRequest defines a SOQL query request, and is usable as a composite
// sub-request.
type QueryRequest struct {
	url string
}

type QueryColumnMetadataRequest struct {
	QueryRequest
}

func NewQueryRequest(service session.ServiceFormatter, querier QueryFormatter, all bool) (*QueryRequest, error) {
	query, err := querier.Format()
	if err != nil {
		return nil, err
	}

	endpoint := "query"
	if all {
		endpoint += "All"
	}

	form := url.Values{}
	form.Add("q", query)

	return &QueryRequest{
		url: strings.TrimPrefix(
			fmt.Sprintf("%s/%s/?%s", service.DataServiceURL(), endpoint, form.Encode()),
			service.InstanceURL(),
		),
	}, nil
}

func (q *QueryRequest) URL() string {
	return q.url
}

func (q *QueryRequest) Method() string {
	return http.MethodGet
}

func (q *QueryRequest) UnmarshalResponse(result batch.Subvalue) (QueryResponse, error) {
	if result.StatusCode != http.StatusOK {
		return QueryResponse{}, batch.HandleSubrequestError(result)
	}

	var resp QueryResponse
	err := json.Unmarshal(result.Result, &resp)
	if err != nil {
		return QueryResponse{}, err
	}

	return resp, nil
}

func NewQueryColumnMetadataRequest(service session.ServiceFormatter, querier QueryFormatter) (*QueryColumnMetadataRequest, error) {
	query, err := querier.Format()
	if err != nil {
		return nil, err
	}

	endpoint := "query"

	form := url.Values{}
	form.Add("q", query)
	form.Add("columns", "true")

	return &QueryColumnMetadataRequest{
		QueryRequest: QueryRequest{
			url: strings.TrimPrefix(
				fmt.Sprintf("%s/%s/?%s", service.DataServiceURL(), endpoint, form.Encode()),
				service.InstanceURL(),
			),
		},
	}, nil
}

func (q *QueryColumnMetadataRequest) UnmarshalResponse(result batch.Subvalue) (QueryColumnMetadataResposne, error) {
	if result.StatusCode != http.StatusOK {
		return QueryColumnMetadataResposne{}, batch.HandleSubrequestError(result)
	}

	var resp QueryColumnMetadataResposne
	err := json.Unmarshal(result.Result, &resp)
	if err != nil {
		return QueryColumnMetadataResposne{}, err
	}

	return resp, nil
}
