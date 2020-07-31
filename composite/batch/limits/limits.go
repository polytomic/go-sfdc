// Package limits provides a batch Subrequester that fetches account limit
// information
package limits

import (
	"fmt"
	"net/http"
)

// LimitRequest provides an batch subrequester that will fetch the current
// account limits
type LimitRequest struct {
	version int
}

// NewSubrequester returns a new limit subrequester
func NewSubrequester(version int) *LimitRequest {
	return &LimitRequest{
		version: version,
	}
}

// URL returns the URL for the limits request
func (l *LimitRequest) URL() string {
	return fmt.Sprintf("/v%d.0/limits", l.version)
}

// Method returns the HTTP method for the limits request
func (l *LimitRequest) Method() string {
	return http.MethodGet
}

// BinaryPartName fulfills batch.Subrequester; it is unused for limits requests
func (l *LimitRequest) BinaryPartName() string {
	return ""
}

// BinaryPartNameAlias fulfills batch.Subrequester; it is unused for limits
// requests
func (l *LimitRequest) BinaryPartNameAlias() string {
	return ""
}

// RichInput fulfills batch.Subrequester; it is unused for limits requests
func (l *LimitRequest) RichInput() map[string]interface{} {
	return nil
}
