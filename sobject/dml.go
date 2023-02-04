package sobject

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
)

// InsertValue is the value that is returned when a
// record is inserted into Salesforce.
type InsertValue struct {
	Success bool         `json:"success"`
	ID      string       `json:"id"`
	Errors  []sfdc.Error `json:"errors"`
}

// UpsertValue is the value that is return when a
// record as been upserted into Salesforce.
//
// Upsert will return two types of values, which
// are indicated by Inserted.  If Created is true,
// then the InsertValue is populated.
type UpsertValue struct {
	Created bool `json:"created"`
	InsertValue
}

// Inserter provides the parameters needed insert a record.
//
// SObject is the Salesforce table name.  An example would be Account or Custom__c.
//
// Fields are the fields of the record that are to be inserted.  It is the
// callers responsibility to provide value fields and values.
type Inserter interface {
	SObject() string
	Fields() map[string]interface{}
}

// Updater provides the parameters needed to update a record.
//
// SObject is the Salesforce table name.  An example would be Account or Custom__c.
//
// ID is the Salesforce ID that will be updated.
//
// Fields are the fields of the record that are to be inserted.  It is the
// callers responsibility to provide value fields and values.
type Updater interface {
	Inserter
	ID() string
}

// Upserter provides the parameters needed to upsert a record.
//
// SObject is the Salesforce table name.  An example would be Account or Custom__c.
//
// ID is the External ID that will be updated.
//
// ExternalField is the external ID field.
//
// Fields are the fields of the record that are to be inserted.  It is the
// callers responsibility to provide value fields and values.
type Upserter interface {
	Updater
	ExternalField() string
}

// Deleter provides the parameters needed to delete a record.
//
// SObject is the Salesforce table name.  An example would be Account or Custom__c.
//
// ID is the Salesforce ID to be deleted.
type Deleter interface {
	SObject() string
	ID() string
}

type dml struct {
	session session.ServiceFormatter
}

func (d *dml) insertCallout(ctx context.Context, inserter Inserter) (InsertValue, error) {
	request, err := d.insertRequest(ctx, inserter)

	if err != nil {
		return InsertValue{}, err
	}

	value, err := d.insertResponse(request)

	if err != nil {
		return InsertValue{}, err
	}

	return value, nil
}
func (d *dml) insertRequest(ctx context.Context, inserter Inserter) (*http.Request, error) {

	url := d.session.DataServiceURL() + objectEndpoint + inserter.SObject()

	body, err := json.Marshal(inserter.Fields())
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	d.session.AuthorizationHeader(request)
	return request, nil

}

func (d *dml) insertResponse(request *http.Request) (InsertValue, error) {
	response, err := d.session.Client().Do(request)

	if err != nil {
		return InsertValue{}, err
	}

	decoder := json.NewDecoder(response.Body)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return InsertValue{}, sfdc.HandleError(response)
	}

	var value InsertValue
	err = decoder.Decode(&value)
	if err != nil {
		return InsertValue{}, err
	}

	return value, nil
}

func (d *dml) updateCallout(ctx context.Context, updater Updater) error {
	request, err := d.updateRequest(ctx, updater)

	if err != nil {
		return err
	}

	return d.updateResponse(request)

}

func (d *dml) updateRequest(ctx context.Context, updater Updater) (*http.Request, error) {

	url := d.session.DataServiceURL() + objectEndpoint + updater.SObject() + "/" + updater.ID()

	body, err := json.Marshal(updater.Fields())
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))

	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	d.session.AuthorizationHeader(request)
	return request, nil

}

func (d *dml) updateResponse(request *http.Request) error {
	response, err := d.session.Client().Do(request)

	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return sfdc.HandleError(response)
	}

	return nil
}

func (d *dml) upsertCallout(ctx context.Context, upserter Upserter) (UpsertValue, error) {
	request, err := d.upsertRequest(ctx, upserter)

	if err != nil {
		return UpsertValue{}, err
	}

	value, err := d.upsertResponse(request)

	if err != nil {
		return UpsertValue{}, err
	}

	return value, nil
}

func (d *dml) upsertRequest(ctx context.Context, upserter Upserter) (*http.Request, error) {
	url := d.session.DataServiceURL() + objectEndpoint + upserter.SObject() + "/" + upserter.ExternalField() + "/" + upserter.ID()

	// TODO: switch to json.NewEncoder():
	body, err := json.Marshal(upserter.Fields())
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")
	d.session.AuthorizationHeader(request)
	return request, nil
}

func (d *dml) upsertResponse(request *http.Request) (UpsertValue, error) {
	response, err := d.session.Client().Do(request)
	if err != nil {
		return UpsertValue{}, err
	}

	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)

	var value UpsertValue
	switch response.StatusCode {
	case http.StatusCreated, http.StatusOK:
		err = decoder.Decode(&value)
		if err != nil {
			return UpsertValue{}, err
		}

	case http.StatusNoContent:
		break // out of the switch

	default:
		return UpsertValue{}, sfdc.HandleError(response)
	}

	return value, nil
}

func (d *dml) deleteCallout(ctx context.Context, deleter Deleter) error {

	request, err := d.deleteRequest(ctx, deleter)

	if err != nil {
		return err
	}

	return d.deleteResponse(request)
}

func (d *dml) deleteRequest(ctx context.Context, deleter Deleter) (*http.Request, error) {

	url := d.session.DataServiceURL() + objectEndpoint + deleter.SObject() + "/" + deleter.ID()

	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)

	if err != nil {
		return nil, err
	}

	d.session.AuthorizationHeader(request)
	return request, nil

}

func (d *dml) deleteResponse(request *http.Request) error {
	response, err := d.session.Client().Do(request)

	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete has failed %d %s", response.StatusCode, response.Status)
	}

	return nil
}
