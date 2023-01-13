package bulk

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
	"github.com/stretchr/testify/require"
)

func TestResource_CreateQueryJob(t *testing.T) {
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
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query"),
						returnStatus(http.StatusOK),
						returnBody(`{
							"apiVersion": 44.0,
							"columnDelimiter": "COMMA",
							"concurrencyMode": "Parallel",
							"contentType": "CSV",
							"createdById": "1234",
							"createdDate": "1/1/1970",
							"id": "9876",
							"object": "Account",
							"operation": "query",
							"state": "UploadComplete"
						}`),
					),
				},
			},
			args: args{
				options: Options{
					Query:           "SELECT Id, Name FROM Account",
					ColumnDelimiter: Comma,
					ContentType:     CSV,
					LineEnding:      Linefeed,
					Operation:       Query,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewResource(V2QueryEndpoint, tt.fields.session)
			require.NoError(t, err)

			_, err = r.CreateJob(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.CreateJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
func TestResource_GetQueryJob(t *testing.T) {
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
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query/123"),
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
			r, err := NewResource(V2QueryEndpoint, tt.fields.session)
			require.NoError(t, err)

			_, err = r.GetJob("123")
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.GetJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
func TestResource_AllQueryJobs(t *testing.T) {
	mockSession := &session.Mock{
		URL: "https://test.salesforce.com",
		HTTPClient: mockHTTPClient(
			expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query?isPkChunkingEnabled=false&jobType=V2Query"),
			returnStatus(http.StatusOK),
			returnBody(`{
				"done": true,
				"records": [
					{
						"id" : "750R0000000zhfdIAA",
						"operation" : "query",
						"object" : "Account",
						"createdById" : "005R0000000GiwjIAC",
						"createdDate" : "2018-12-07T19:58:09.000+0000",
						"systemModstamp" : "2018-12-07T19:59:14.000+0000",
						"state" : "JobComplete",
						"concurrencyMode" : "Parallel",
						"contentType" : "CSV",
						"apiVersion" : 56.0,
						"jobType" : "V2Query",
						"lineEnding" : "LF",
						"columnDelimiter" : "COMMA"
					 },
					 {
						"id" : "750R0000000zhjzIAA",
						"operation" : "query",
						"object" : "Account",
						"createdById" : "005R0000000GiwjIAC",
						"createdDate" : "2018-12-07T20:52:28.000+0000",
						"systemModstamp" : "2018-12-07T20:53:15.000+0000",
						"state" : "JobComplete",
						"concurrencyMode" : "Parallel",
						"contentType" : "CSV",
						"apiVersion" : 56.0,
						"jobType" : "V2Query",
						"lineEnding" : "LF",
						"columnDelimiter" : "COMMA"
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
					JobType: V2Query,
				},
			},
			want: &Jobs{
				session: mockSession,
				response: jobResponse{
					Done: true,
					Records: []Response{
						{
							ID:              "750R0000000zhfdIAA",
							Operation:       "query",
							Object:          "Account",
							CreatedByID:     "005R0000000GiwjIAC",
							CreatedDate:     "2018-12-07T19:58:09.000+0000",
							SystemModstamp:  "2018-12-07T19:59:14.000+0000",
							State:           "JobComplete",
							ConcurrencyMode: "Parallel",
							ContentType:     "CSV",
							APIVersion:      56.0,
							JobType:         "V2Query",
							LineEnding:      "LF",
							ColumnDelimiter: "COMMA",
						},
						{
							ID:              "750R0000000zhjzIAA",
							Operation:       "query",
							Object:          "Account",
							CreatedByID:     "005R0000000GiwjIAC",
							CreatedDate:     "2018-12-07T20:52:28.000+0000",
							SystemModstamp:  "2018-12-07T20:53:15.000+0000",
							State:           "JobComplete",
							ConcurrencyMode: "Parallel",
							ContentType:     "CSV",
							APIVersion:      56.0,
							JobType:         "V2Query",
							LineEnding:      "LF",
							ColumnDelimiter: "COMMA",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewResource(V2QueryEndpoint, tt.fields.session)
			require.NoError(t, err)

			got, err := r.AllJobs(tt.args.parameters)
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
