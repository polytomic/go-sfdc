package collections

import (
	"bytes"
	"context"
	"net/http"

	"github.com/namely/go-sfdc/v3/session"
	"github.com/namely/go-sfdc/v3/sobject"
)

// UpdateValue is the return value from the
// Salesforce API.
type UpdateValue struct {
	sobject.InsertValue
}

type update struct {
	session session.ServiceFormatter
}

func (u *update) callout(ctx context.Context, allOrNone bool, records []sobject.Updater) ([]UpdateValue, error) {
	payload, err := u.payload(allOrNone, records)
	if err != nil {
		return nil, err
	}
	c := &collection{
		method:      http.MethodPatch,
		body:        payload,
		endpoint:    endpoint,
		contentType: jsonContentType,
	}
	var values []UpdateValue
	err = c.send(ctx, u.session, &values)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (u *update) payload(allOrNone bool, recs []sobject.Updater) (*bytes.Reader, error) {
	records := make([]interface{}, len(recs))
	for idx, updater := range recs {
		rec := map[string]interface{}{
			"attributes": map[string]string{
				"type": updater.SObject(),
			},
		}
		for field, value := range updater.Fields() {
			rec[field] = value
		}
		rec["id"] = updater.ID()
		records[idx] = rec
	}
	return dmlpayload(allOrNone, records)
}
