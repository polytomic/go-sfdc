package bulk

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
	"github.com/stretchr/testify/require"
)

func TestNewResource(t *testing.T) {
	type args struct {
		session session.ServiceFormatter
	}
	tests := []struct {
		name    string
		args    args
		want    *Resource
		wantErr bool
	}{
		{
			name: "Created",
			args: args{
				session: &session.Mock{},
			},
			want: &Resource{
				session:  &session.Mock{},
				endpoint: V2IngestEndpoint,
			},
			wantErr: false,
		},
		{
			name:    "failed",
			args:    args{},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewResource(context.Background(), V2IngestEndpoint, tt.args.session)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewResource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResource_CreateJob(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest"),
						returnStatus(http.StatusOK),
						returnBody(`{
							"apiVersion": 44.0,
							"columnDelimiter": "COMMA",
							"concurrencyMode": "Parallel",
							"contentType": "CSV",
							"contentUrl": "services/v44.0/jobs",
							"createdById": "1234",
							"createdDate": "1/1/1970",
							"externalIdFieldName": "namename",
							"id": "9876",
							"jobType": "V2Ingest",
							"lineEnding": "LF",
							"object": "Account",
							"operation": "Insert",
							"state": "Open",
							"systemModstamp": "1/1/1980"
						}`,
						),
					),
				},
			},
			args: args{
				options: Options{
					ColumnDelimiter:     Comma,
					ContentType:         CSV,
					ExternalIDFieldName: "Some External Field",
					LineEnding:          Linefeed,
					Object:              "Account",
					Operation:           Insert,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewResource(context.Background(), V2IngestEndpoint, tt.fields.session)
			require.NoError(t, err)

			_, err = r.CreateJob(context.Background(), tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.CreateJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
func TestResource_GetJob(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/123"),
						returnStatus(http.StatusOK),
						returnBody(`{
							"apiVersion": 44.0,
							"columnDelimiter": "COMMA",
							"concurrencyMode": "Parallel",
							"contentType": "CSV",
							"contentUrl": "services/v44.0/jobs",
							"createdById": "1234",
							"createdDate": "1/1/1970",
							"externalIdFieldName": "namename",
							"id": "9876",
							"jobType": "V2Ingest",
							"lineEnding": "LF",
							"object": "Account",
							"operation": "Insert",
							"state": "Open",
							"systemModstamp": "1/1/1980"
						}`,
						),
					),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewResource(context.Background(), V2IngestEndpoint, tt.fields.session)
			require.NoError(t, err)

			_, err = r.GetJob(context.Background(), "123")
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.GetJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
func TestResource_AllJobs(t *testing.T) {
	mockSession := &session.Mock{
		URL: "https://test.salesforce.com",
		HTTPClient: mockHTTPClient(
			expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest?isPkChunkingEnabled=false&jobType=V2Ingest"),
			returnStatus(http.StatusOK),
			returnBody(`{
				"done": true,
				"records": [
					{
						"apiVersion": 44.0,
						"columnDelimiter": "COMMA",
						"concurrencyMode": "Parallel",
						"contentType": "CSV",
						"contentUrl": "services/v44.0/jobs",
						"createdById": "1234",
						"createdDate": "1/1/1970",
						"externalIdFieldName": "namename",
						"id": "9876",
						"jobType": "V2Ingest",
						"lineEnding": "LF",
						"object": "Account",
						"operation": "Insert",
						"state": "Open",
						"systemModstamp": "1/1/1980"
					}
				]
			}`,
			),
		),
	}

	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		parameters Parameters
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Jobs
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: mockSession,
			},
			args: args{
				parameters: Parameters{
					JobType: V2Ingest,
				},
			},
			want: &Jobs{
				session: mockSession,
				response: jobResponse{
					Done: true,
					Records: []Response{
						{
							APIVersion:          44.0,
							ColumnDelimiter:     "COMMA",
							ConcurrencyMode:     "Parallel",
							ContentType:         "CSV",
							ContentURL:          "services/v44.0/jobs",
							CreatedByID:         "1234",
							CreatedDate:         "1/1/1970",
							ExternalIDFieldName: "namename",
							ID:                  "9876",
							JobType:             "V2Ingest",
							LineEnding:          "LF",
							Object:              "Account",
							Operation:           "Insert",
							State:               "Open",
							SystemModstamp:      "1/1/1980",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewResource(context.Background(), V2IngestEndpoint, tt.fields.session)
			require.NoError(t, err)

			got, err := r.AllJobs(context.Background(), tt.args.parameters)
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.AllJobs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Resource.AllJobs() = %v, want %v", got, tt.want)
			}
		})
	}
}
