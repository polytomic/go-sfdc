// Package limits provides a batch Subrequester that fetches account limit
// information
package limits

import (
	"fmt"
	"net/http"

	"github.com/namely/go-sfdc/v3/session"
)

const (
	LimitDailyApiRequests = "DailyApiRequests"
)

// LimitRequest provides an batch subrequester that will fetch the current
// account limits
type LimitRequest struct {
	version int
	sess    session.ServiceFormatter
}

// NewSubrequester returns a new limit subrequester
func NewSubrequester(sess session.ServiceFormatter) *LimitRequest {
	return NewSubrequesterWithVersion(sess, sess.Version())
}

// NewSubrequesterWithVersion returns a new limit subrequester with a specific
// API version
func NewSubrequesterWithVersion(sess session.ServiceFormatter, version int) *LimitRequest {
	return &LimitRequest{sess: sess, version: version}
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
