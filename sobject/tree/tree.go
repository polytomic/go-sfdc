package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
)

// Inserter is used to define the SObject and it's records for the
// composite tree API.
type Inserter interface {
	SObject() string
	Records() []*Record
}

// InsertValue is the return value for each record.
type InsertValue struct {
	ReferenceID string       `json:"referenceId"`
	ID          string       `json:"id"`
	Errors      []sfdc.Error `json:"errors"`
}

// Value is the return value from the API call.
type Value struct {
	HasErrors bool          `json:"hasErrors"`
	Results   []InsertValue `json:"results"`
}

// Resource is the composite tree API resource.
type Resource struct {
	session session.ServiceFormatter
}

const objectEndpoint = "/composite/tree/"

// NewResource creates a new composite tree resource from the session.
func NewResource(ctx context.Context, session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("sobject tree: session can not be nil")
	}

	err := session.Refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("session refresh: %w", err)
	}

	return &Resource{
		session: session,
	}, nil
}

// Insert will call the composite tree API.
func (r *Resource) Insert(ctx context.Context, inserter Inserter) (*Value, error) {
	if inserter == nil {
		return nil, errors.New("tree resourse: inserter can not be nil")
	}
	sobject := inserter.SObject()
	matching, err := regexp.MatchString(`\w`, sobject)
	if err != nil {
		return nil, err
	}
	if !matching {
		return nil, fmt.Errorf("tree resourse: %s is not a valid sobject", sobject)
	}

	return r.callout(ctx, inserter)
}
func (r *Resource) callout(ctx context.Context, inserter Inserter) (*Value, error) {

	request, err := r.request(ctx, inserter)

	if err != nil {
		return nil, err
	}

	value, err := r.response(request)

	if err != nil {
		return nil, err
	}

	return &value, nil
}
func (r *Resource) request(ctx context.Context, inserter Inserter) (*http.Request, error) {

	url := r.session.DataServiceURL() + objectEndpoint + inserter.SObject()

	body, err := r.payload(inserter)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	r.session.AuthorizationHeader(request)
	return request, nil

}

func (r *Resource) payload(inserter Inserter) (*bytes.Reader, error) {
	records := struct {
		Records []*Record `json:"records"`
	}{
		Records: inserter.Records(),
	}
	payload, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(payload), nil
}

func (r *Resource) response(request *http.Request) (Value, error) {
	response, err := r.session.Client().Do(request)
	if err != nil {
		return Value{}, err
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)

	if response.StatusCode != http.StatusCreated {
		return Value{}, sfdc.HandleError(response)
	}

	var value Value
	err = decoder.Decode(&value)
	if err != nil {
		return Value{}, err
	}

	return value, nil
}
