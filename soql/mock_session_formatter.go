package soql

import (
	"fmt"
	"net/http"
)

type mockSessionFormatter struct {
	url        string
	client     *http.Client
	refreshErr error
}

func (mock *mockSessionFormatter) DataServiceURL() string {
	return fmt.Sprintf("%s/services/data/v%d.0", mock.InstanceURL(), mock.Version())
}

func (mock *mockSessionFormatter) Version() int {
	return 42
}

func (mock *mockSessionFormatter) AuthorizationHeader(*http.Request) {}

func (mock *mockSessionFormatter) Client() *http.Client {
	return mock.client
}

func (mock *mockSessionFormatter) InstanceURL() string {
	return mock.url
}

func (mock *mockSessionFormatter) Refresh() error {
	return mock.refreshErr
}
