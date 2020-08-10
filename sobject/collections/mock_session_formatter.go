package collections

import "net/http"

type mockSessionFormatter struct {
	url        string
	client     *http.Client
	refreshErr error
}

func (mock *mockSessionFormatter) ServiceURL() string {
	return mock.url
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
