package bulk

import (
	"io"
	"net/http"
	"strings"
)

type mockSettings struct {
	expectURL     string
	expectMethod  string
	expectHeaders map[string]string

	statusCode int
	body       string
	headers    map[string]string
}

func (m *mockSettings) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.expectURL != "" && m.expectURL != req.URL.String() {
		return &http.Response{
			StatusCode: 500,
			Status:     "Invalid URL",
			Body:       io.NopCloser(strings.NewReader(req.URL.String())),
			Header:     make(http.Header),
		}, nil
	}

	if m.expectMethod != "" && m.expectMethod != req.Method {
		return &http.Response{
			StatusCode: 500,
			Status:     "Invalid method",
			Body:       io.NopCloser(strings.NewReader(req.Method)),
			Header:     make(http.Header),
		}, nil
	}

	if m.expectHeaders != nil {
		for k, v := range m.expectHeaders {
			if req.Header.Get(k) != v {
				return &http.Response{
					StatusCode: 500,
					Status:     "Invalid header",
					Body:       io.NopCloser(strings.NewReader(req.Header.Get(k))),
					Header:     make(http.Header),
				}, nil
			}
		}
	}

	headers := make(http.Header)
	for k, v := range m.headers {
		headers.Set(k, v)
	}
	return &http.Response{
		StatusCode: m.statusCode,
		Status:     http.StatusText(m.statusCode),
		Body:       io.NopCloser(strings.NewReader(m.body)),
		Header:     headers,
	}, nil
}

func mockHTTPClient(opts ...mockConfig) *http.Client {
	mockConf := &mockSettings{}
	for _, opt := range opts {
		opt(mockConf)
	}
	return &http.Client{
		Transport: mockConf,
	}
}

type mockConfig func(*mockSettings)

func expectMethod(method string) mockConfig {
	return func(conf *mockSettings) {
		conf.expectMethod = method
	}
}
func expectURL(urls string) mockConfig {
	return func(conf *mockSettings) {
		conf.expectURL = urls
	}
}

func expectHeader(header, value string) mockConfig {
	return func(conf *mockSettings) {
		if conf.expectHeaders == nil {
			conf.expectHeaders = make(map[string]string)
		}
		conf.expectHeaders[header] = value
	}
}

func returnStatus(status int) mockConfig {
	return func(conf *mockSettings) {
		conf.statusCode = status
	}
}

func returnBody(body string) mockConfig {
	return func(conf *mockSettings) {
		conf.body = body
	}
}

func returnHeader(header, value string) mockConfig {
	return func(conf *mockSettings) {
		if conf.headers == nil {
			conf.headers = make(map[string]string)
		}
		conf.headers[header] = value
	}
}
