package bulkv1

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type roundTripFunc func(request *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

type mockHTTPFilter func(*http.Request) *http.Response

func wantURL(url string) mockHTTPFilter {
	return func(req *http.Request) *http.Response {
		if req.URL.String() != url {
			return &http.Response{
				StatusCode: 500,
				Status:     fmt.Sprintf("Invalid URL; expected %s", url),
				Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
				Header:     make(http.Header),
			}
		}
		return nil
	}
}

func wantMethod(method string) mockHTTPFilter {
	return func(req *http.Request) *http.Response {
		if req.Method != method {
			return &http.Response{
				StatusCode: 500,
				Status:     "Invalid Method",
				Body:       ioutil.NopCloser(strings.NewReader(req.Method)),
				Header:     make(http.Header),
			}
		}
		return nil
	}
}

func mockHTTPClient(fn roundTripFunc, filters ...mockHTTPFilter) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(
			func(req *http.Request) *http.Response {
				for _, f := range filters {
					if resp := f(req); resp != nil {
						return resp
					}
				}
				return fn(req)
			},
		),
	}
}

type mock struct {
	fn   roundTripFunc
	cond []mockHTTPFilter
}

func multiMockHTTPClient(mocks map[string]mock) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(
			func(req *http.Request) *http.Response {
				if mock, ok := mocks[req.URL.String()]; ok {
					for _, f := range mock.cond {
						if resp := f(req); resp != nil {
							return resp
						}
					}
					return mock.fn(req)
				}
				return &http.Response{
					StatusCode: 500,
					Status:     "Invalid URL",
					Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
					Header:     make(http.Header),
				}
			},
		),
	}
}
