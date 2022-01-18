package batch

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
	"github.com/pkg/errors"
)

// Subrequester provides the composite batch API requests.
type Subrequester interface {
	URL() string
	Method() string
}

type BinarySubrequester interface {
	BinaryPartName() string
	BinaryPartNameAlias() string
	RichInput() map[string]interface{}
}

// Value is the returned structure from the composite batch API response.
type Value struct {
	HasErrors bool       `json:"hasErrors"`
	Results   []Subvalue `json:"results"`
}

// Subvalue is the subresponses to the composite batch API.
type Subvalue struct {
	Result     json.RawMessage `json:"result"`
	StatusCode int             `json:"statusCode"`
}

const endpoint = "/composite/batch"

var validMethods = map[string]struct{}{
	"PUT":    {},
	"POST":   {},
	"PATCH":  {},
	"GET":    {},
	"DELETE": {},
}

// Resource is the structure that can be just to call composite batch APIs.
type Resource struct {
	session session.ServiceFormatter
}

// NewResource creates a new resourse with the session.  If the session is
// nil an error will be returned.
func NewResource(session session.ServiceFormatter) (*Resource, error) {
	if session == nil {
		return nil, errors.New("composite: session can not be nil")
	}

	err := session.Refresh()
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resource{
		session: session,
	}, nil
}

// Retrieve will retrieve the responses to a composite batch requests.  The
// order of the array is the order in which the subrequests are
// placed in the composite batch body.
func (r *Resource) Retrieve(haltOnError bool, requesters []Subrequester) (Value, error) {
	if requesters == nil {
		return Value{}, errors.New("composite subrequests: requesters can not nil")
	}
	err := r.validateSubrequests(requesters)
	if err != nil {
		return Value{}, err
	}

	body, err := r.payload(haltOnError, requesters)
	if err != nil {
		return Value{}, err
	}

	url := r.session.DataServiceURL() + endpoint

	request, err := http.NewRequest(http.MethodPost, url, body)

	if err != nil {
		return Value{}, err
	}

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	r.session.AuthorizationHeader(request)

	response, err := r.session.Client().Do(request)

	if err != nil {
		return Value{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return Value{}, sfdc.HandleError(response)
	}

	var value Value
	err = decoder.Decode(&value)
	if err != nil {
		return Value{}, err
	}
	return value, nil
}

func (r *Resource) validateSubrequests(requesters []Subrequester) error {
	for _, requester := range requesters {
		if requester.URL() == "" {
			return errors.New("composite subrequest: must contain a url")
		}
		if _, has := validMethods[requester.Method()]; !has {
			return errors.New("composite subrequest: empty or invalid method " + requester.Method())
		}
	}
	return nil
}
func (r *Resource) payload(haltOnError bool, requesters []Subrequester) (*bytes.Reader, error) {
	subRequests := make([]interface{}, len(requesters))
	for idx, requester := range requesters {
		subRequest := map[string]interface{}{
			"url":    requester.URL(),
			"method": requester.Method(),
		}
		if binarySub, ok := requester.(BinarySubrequester); ok {
			if binarySub.BinaryPartName() != "" {
				subRequest["binaryPartName"] = binarySub.BinaryPartName()
			}
			if binarySub.BinaryPartNameAlias() != "" {
				subRequest["binaryPartNameAlias"] = binarySub.BinaryPartNameAlias()
			}
			if binarySub.RichInput() != nil {
				subRequest["richInput"] = binarySub.RichInput()
			}
		}
		subRequests[idx] = subRequest
	}
	payload := map[string]interface{}{
		"haltOnError":   haltOnError,
		"batchRequests": subRequests,
	}
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(jsonBody), nil
}
