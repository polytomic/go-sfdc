package sobject

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
	"github.com/pkg/errors"
)

// ObjectURLs is the URL for the SObject metadata.
type ObjectURLs struct {
	CompactLayouts   string `json:"compactLayouts"`
	RowTemplate      string `json:"rowTemplate"`
	ApprovalLayouts  string `json:"approvalLayouts"`
	DefaultValues    string `json:"defaultValues"`
	ListViews        string `json:"listviews"`
	Describe         string `json:"describe"`
	QuickActions     string `json:"quickActions"`
	Layouts          string `json:"layouts"`
	SObject          string `json:"sobject"`
	UIDetailTemplate string `json:"uiDetailTemplate"`
	UIEditTemplate   string `json:"uiEditTemplate"`
	UINewRecord      string `json:"uiNewRecord"`
}

// Resources is the structure for the Salesforce APIs for SObjects.
type Resources struct {
	metadata *metadata
	describe *describe
	list     *list
	dml      *dml
	query    *query
}

const objectEndpoint = "/sobjects/"

// NewResources forms the Salesforce SObject resource structure.  The
// session formatter is required to form the proper URLs and authorization
// header.
func NewResources(ctx context.Context, session session.ServiceFormatter) (*Resources, error) {
	if session == nil {
		return nil, errors.New("sobject resource: session can not be nil")
	}

	err := session.Refresh(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "session refresh")
	}

	return &Resources{
		metadata: &metadata{
			session: session,
		},
		describe: &describe{
			session: session,
		},
		list: &list{
			session: session,
		},
		dml: &dml{
			session: session,
		},
		query: &query{
			session: session,
		},
	}, nil
}

// List returns the list of sObjects available
func (r *Resources) List(ctx context.Context) (ListValue, error) {
	if r.list == nil {
		return ListValue{}, errors.New("salesforce api is not initialized properly")
	}

	return r.list.callout(ctx)
}

// Metadata retrieves the SObject's metadata.
func (r *Resources) Metadata(ctx context.Context, sobject string) (MetadataValue, error) {
	if r.metadata == nil {
		return MetadataValue{}, errors.New("salesforce api is not initialized properly")
	}

	matching, err := regexp.MatchString(`\w`, sobject)
	if err != nil {
		return MetadataValue{}, err
	}

	if matching == false {
		return MetadataValue{}, fmt.Errorf("sobject salesforce api: %s is not a valid sobject", sobject)
	}

	return r.metadata.callout(ctx, sobject)
}

// Describe retrieves the SObject's describe.
func (r *Resources) Describe(ctx context.Context, sobject string) (DescribeValue, error) {
	if r.describe == nil {
		return DescribeValue{}, errors.New("salesforce api is not initialized properly")
	}

	matching, err := regexp.MatchString(`\w`, sobject)
	if err != nil {
		return DescribeValue{}, err
	}

	if matching == false {
		return DescribeValue{}, fmt.Errorf("sobject salesforce api: %s is not a valid sobject", sobject)
	}

	return r.describe.callout(ctx, sobject)
}

// Insert will create a new Salesforce record.
func (r *Resources) Insert(ctx context.Context, inserter Inserter) (InsertValue, error) {
	if r.dml == nil {
		return InsertValue{}, errors.New("salesforce api is not initialized properly")
	}

	if inserter == nil {
		return InsertValue{}, errors.New("inserter can not be nil")
	}

	return r.dml.insertCallout(ctx, inserter)

}

// Update will update an existing Salesforce record.
func (r *Resources) Update(ctx context.Context, updater Updater) error {
	if r.dml == nil {
		return errors.New("salesforce api is not initialized properly")
	}

	if updater == nil {
		return errors.New("updater can not be nil")
	}

	return r.dml.updateCallout(ctx, updater)

}

// Upsert will upsert an existing or new Salesforce record.
func (r *Resources) Upsert(ctx context.Context, upserter Upserter) (UpsertValue, error) {
	if r.dml == nil {
		return UpsertValue{}, errors.New("salesforce api is not initialized properly")
	}

	if upserter == nil {
		return UpsertValue{}, errors.New("upserter can not be nil")
	}

	return r.dml.upsertCallout(ctx, upserter)

}

// Delete will delete an existing Salesforce record.
func (r *Resources) Delete(ctx context.Context, deleter Deleter) error {
	if r.dml == nil {
		return errors.New("salesforce api is not initialized properly")
	}

	if deleter == nil {
		return errors.New("deleter can not be nil")
	}

	return r.dml.deleteCallout(ctx, deleter)
}

// Query returns a SObject record using the Salesforce ID.
func (r *Resources) Query(ctx context.Context, querier Querier) (*sfdc.Record, error) {
	if r.query == nil {
		return nil, errors.New("salesforce api is not initialized properly")
	}

	if querier == nil {
		return nil, errors.New("querier can not be nil")
	}

	return r.query.callout(ctx, querier)
}

// ExternalQuery returns a SObject record using an external ID field.
func (r *Resources) ExternalQuery(ctx context.Context, querier ExternalQuerier) (*sfdc.Record, error) {
	if r.query == nil {
		return nil, errors.New("salesforce api is not initialized properly")
	}

	if querier == nil {
		return nil, errors.New("querier can not be nil")
	}

	return r.query.externalCallout(ctx, querier)
}

// DeletedRecords returns a list of records that have been deleted from a date range.
func (r *Resources) DeletedRecords(ctx context.Context, sobject string, startDate, endDate time.Time) (DeletedRecords, error) {
	if r.query == nil {
		return DeletedRecords{}, errors.New("salesforce api is not initialized properly")
	}

	matching, err := regexp.MatchString(`\w`, sobject)
	if err != nil {
		return DeletedRecords{}, err
	}

	if matching == false {
		return DeletedRecords{}, fmt.Errorf("sobject salesforce api: %s is not a valid sobject", sobject)
	}

	return r.query.deletedRecordsCallout(ctx, sobject, startDate, endDate)
}

// UpdatedRecords returns a list of records that have been updated from a date range.
func (r *Resources) UpdatedRecords(ctx context.Context, sobject string, startDate, endDate time.Time) (UpdatedRecords, error) {
	if r.query == nil {
		return UpdatedRecords{}, errors.New("salesforce api is not initialized properly")
	}

	matching, err := regexp.MatchString(`\w`, sobject)
	if err != nil {
		return UpdatedRecords{}, err
	}

	if matching == false {
		return UpdatedRecords{}, fmt.Errorf("sobject salesforce api: %s is not a valid sobject", sobject)
	}

	return r.query.updatedRecordsCallout(ctx, sobject, startDate, endDate)
}

// GetContent returns the blob from a content SObject.
func (r *Resources) GetContent(ctx context.Context, id string, content ContentType) ([]byte, error) {
	if r.query == nil {
		return nil, errors.New("salesforce api is not initialized properly")
	}

	if id == "" {
		return nil, fmt.Errorf("sobject salesforce api: %s can not be empty", id)
	}

	switch content {
	case AttachmentType:
	case DocumentType:
	default:
		return nil, fmt.Errorf("sobject salesforce: content type (%s) is not supported", string(content))
	}

	return r.query.contentCallout(ctx, id, content)
}
