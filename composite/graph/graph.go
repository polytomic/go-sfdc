package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/namely/go-sfdc/v3/session"
)

type Subrequest struct {
	URL         string      `json:"url,omitempty"`
	Body        interface{} `json:"body,omitempty"`
	Method      string      `json:"method,omitempty"`
	ReferenceID string      `json:"referenceId,omitempty"`
}

// GraphRequest contains a single graph request which may be part of an HTTP
// request containing many graphs.
type GraphRequest struct {
	GraphID  string       `json:"graphId,omitempty"`
	Requests []Subrequest `json:"compositeRequest,omitempty"`
}

type Request struct {
	Graphs []GraphRequest `json:"graphs,omitempty"`
}

type SubrequestResponse struct {
	Body           json.RawMessage   `json:"body,omitempty"`
	HttpHeaders    map[string]string `json:"httpHeaders,omitempty"`
	HttpStatusCode int               `json:"httpStatusCode,omitempty"`
	ReferenceID    string            `json:"referenceId,omitempty"`
}

type SubrequestResponses struct {
	Responses []SubrequestResponse `json:"compositeResponse,omitempty"`
}

// GraphReqponse contains a single graph response.
type GraphResponse struct {
	GraphID      string              `json:"graphId,omitempty"`
	Result       SubrequestResponses `json:"graphResponse,omitempty"`
	IsSuccessful bool                `json:"isSuccessful,omitempty"`
}

type Response struct {
	Graphs []GraphResponse `json:"graphs,omitempty"`
}

func Do(ctx context.Context, session session.ServiceFormatter, graph Request) (*Response, error) {
	body, err := json.Marshal(graph)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/composite/graph", session.DataServiceURL()), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-type", "application/json")

	resp, err := session.Client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	graphResp := &Response{}
	err = json.NewDecoder(resp.Body).Decode(graphResp)
	return graphResp, err
}

// SuccessResponseBody defines the structure of the composite response body when
// the graph has been successful.
type SuccessReponseBody struct {
	ID      string        `json:"id,omitempty"`
	Success bool          `json:"success,omitempty"`
	Errors  []interface{} `json:"errors,omitempty"`
}

// ErrorResponseBody defines the structure of the composite repsonse body when
// the graph was unsuccessful.
type ErrorResponseBody []CompositeRequestError

// CompositeRequestError contains a single error that occurred during processing
// a graph request.
type CompositeRequestError struct {
	ErrorCode string `json:"errorCode,omitempty"`
	Message   string `json:"message,omitempty"`
}
