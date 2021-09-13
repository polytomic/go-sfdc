package soql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

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
		url: fmt.Sprintf("/services/data/v%d.0/%s/?%s", service.Version(), endpoint, form.Encode()),
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
