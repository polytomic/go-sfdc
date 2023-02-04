package session

import (
	"context"
	"fmt"
	"net/http"
)

var (
	_ ServiceFormatter = (*Mock)(nil)
)

// Mock provides a test implementation of ServiceFormatter.
type Mock struct {
	URL        string
	HTTPClient *http.Client
	RefreshErr error
}

func (mock *Mock) DataServiceURL() string {
	return fmt.Sprintf("%s/services/data/v%d.0", mock.InstanceURL(), mock.Version())
}

func (mock *Mock) Version() int {
	return 42
}

func (mock *Mock) AuthorizationHeader(*http.Request) {}

func (mock *Mock) Client() *http.Client {
	return mock.HTTPClient
}

func (mock *Mock) InstanceURL() string {
	return mock.URL
}

func (mock *Mock) Refresh(context.Context) error {
	return mock.RefreshErr
}
