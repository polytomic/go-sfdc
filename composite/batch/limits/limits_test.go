package limits

import (
	"net/http"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
)

type mockSessionFormatter struct {
	url        string
	client     *http.Client
	refreshErr error
}

func (mock *mockSessionFormatter) ServiceURL() string {
	return mock.url
}

func (mock *mockSessionFormatter) Version() int {
	return 44
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

func TestLimitSubRequestURL(t *testing.T) {
	cases := []struct {
		name    string
		session session.ServiceFormatter
		expect  string
	}{
		{
			name:    "success",
			session: &mockSessionFormatter{url: "https://test.salesforce.com/services/data/v44.0"},
			expect:  "/v44.0/limits",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			limitRequest := NewSubrequester(tt.session)
			if url := limitRequest.URL(); url != tt.expect {
				t.Errorf("Subrequester.URL() got %v, expected %v", url, tt.expect)
			}
		})
	}
}
