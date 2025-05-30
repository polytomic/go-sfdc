package sobject

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
)

// ListValue is a structure that is returned from Salesforce SObject list calls
type ListValue struct {
	SObjects []DescribeValue `json:"sobjects"`
}

type list struct {
	session session.ServiceFormatter
}

func (l *list) callout(ctx context.Context, opts DescribeOptions) (ListValue, error) {
	request, err := l.request(ctx)
	if err != nil {
		return ListValue{}, err
	}
	if !opts.IfModifiedSince.IsZero() {
		request.Header.Add("If-Modified-Since", opts.IfModifiedSince.Format(time.RFC1123))
	}

	value, err := l.response(request)
	if err != nil {
		return ListValue{}, err
	}

	return value, nil
}

func (l *list) request(ctx context.Context) (*http.Request, error) {
	url := l.session.DataServiceURL() + objectEndpoint
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json, */*")
	l.session.AuthorizationHeader(request)
	return request, nil
}

func (l *list) response(request *http.Request) (ListValue, error) {
	response, err := l.session.Client().Do(request)
	if err != nil {
		return ListValue{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotModified {
		return ListValue{}, ErrNotModified
	}
	if response.StatusCode != http.StatusOK {
		return ListValue{}, sfdc.HandleError(response)
	}

	var value ListValue
	err = decoder.Decode(&value)
	if err != nil {
		return ListValue{}, err
	}

	return value, nil
}
