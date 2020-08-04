// Package limits provides a batch Subrequester that fetches account limit
// information
package limits

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/namely/go-sfdc/v3/session"
)

// LimitRequest provides an batch subrequester that will fetch the current
// account limits
type LimitRequest struct {
	sess session.ServiceFormatter
}

// NewSubrequester returns a new limit subrequester
func NewSubrequester(sess session.ServiceFormatter) *LimitRequest {
	return &LimitRequest{sess: sess}
}

// URL returns the URL for the limits request
func (l *LimitRequest) URL() string {
	if urlPieces := strings.Split(l.sess.ServiceURL(), "services/data"); len(urlPieces) > 1 {
		return fmt.Sprintf("%s/limits", urlPieces[1])
	}

	return fmt.Sprintf("%s/limits", l.sess.ServiceURL())
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
