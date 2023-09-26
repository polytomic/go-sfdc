package bulkv1

import (
	"fmt"
	"io"
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
				Body:       io.NopCloser(strings.NewReader(req.URL.String())),
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
				Body:       io.NopCloser(strings.NewReader(req.Method)),
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
