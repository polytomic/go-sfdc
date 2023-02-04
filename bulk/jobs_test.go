package bulk

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
)

func TestJobs_do(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		params  Parameters
	}
	type args struct {
		request *http.Request
	}
	tests := map[string]struct {
		endpoint BulkEndpoint
		fields   fields
		args     args
		want     jobResponse
		wantErr  bool
	}{
		"Passing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				params: Parameters{
					JobType: V2Ingest,
				},
				session: &session.Mock{
					HTTPClient: mockHTTPClient(
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
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: jobResponse{
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
			wantErr: false,
		},
		"passing -- query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				params: Parameters{
					JobType: V2Query,
				},
				session: &session.Mock{
					HTTPClient: mockHTTPClient(
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
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: jobResponse{
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
			wantErr: false,
		},
		"failing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				params: Parameters{
					JobType: V2Ingest,
				},
				session: &session.Mock{
					HTTPClient: mockHTTPClient(
						returnStatus(http.StatusBadRequest),
						returnBody(`[
							{
								"fields" : [ "Id" ],
								"message" : "Account ID: id value of incorrect type: 001900K0001pPuOAAU",
								"errorCode" : "MALFORMED_ID"
							}
						]`,
						),
					),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want:    jobResponse{},
			wantErr: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j, err := newJobs(context.Background(), tt.fields.session, tt.endpoint, tt.fields.params)
			// require.NoError(t, err)
			// got, err := j.do(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Jobs.do() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !(tt.wantErr || reflect.DeepEqual(j.response, tt.want)) {
				t.Errorf("Jobs.do() = %v, want %v", j.response, tt.want)
			}
		})
	}
}

func Test_newJobs(t *testing.T) {
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

	type args struct {
		session    session.ServiceFormatter
		parameters Parameters
	}
	tests := []struct {
		name    string
		args    args
		want    *Jobs
		wantErr bool
	}{
		{
			name: "Passing",
			args: args{
				session: mockSession,
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
			got, err := newJobs(context.Background(), tt.args.session, V2IngestEndpoint, tt.args.parameters)
			if (err != nil) != tt.wantErr {
				t.Errorf("newJobs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newJobs() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_newQueryJobs(t *testing.T) {
	mockSession := &session.Mock{
		URL: "https://test.salesforce.com",
		HTTPClient: mockHTTPClient(
			expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query?isPkChunkingEnabled=false&jobType=V2Query"),
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

	type args struct {
		session    session.ServiceFormatter
		parameters Parameters
	}
	tests := []struct {
		name    string
		args    args
		want    *Jobs
		wantErr bool
	}{
		{
			name: "Passing",
			args: args{
				session: mockSession,
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
							Object:              "Account",
							LineEnding:          "LF",
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
			got, err := newJobs(context.Background(), tt.args.session, V2QueryEndpoint, tt.args.parameters)
			if (err != nil) != tt.wantErr {
				t.Errorf("newJobs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newJobs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobs_Done(t *testing.T) {
	type fields struct {
		session  session.ServiceFormatter
		response jobResponse
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "Passing",
			fields: fields{
				response: jobResponse{
					Done: true,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Jobs{
				session:  tt.fields.session,
				response: tt.fields.response,
			}
			if got := j.Done(); got != tt.want {
				t.Errorf("Jobs.Done() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobs_Records(t *testing.T) {
	type fields struct {
		session  session.ServiceFormatter
		response jobResponse
	}
	tests := []struct {
		name   string
		fields fields
		want   []Response
	}{
		{
			name: "Passing",
			fields: fields{
				response: jobResponse{
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
			want: []Response{
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Jobs{
				session:  tt.fields.session,
				response: tt.fields.response,
			}
			if got := j.Records(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Jobs.Records() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobs_Next(t *testing.T) {
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
		session  session.ServiceFormatter
		response jobResponse
	}
	tests := []struct {
		name    string
		fields  fields
		want    *Jobs
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: mockSession,
				response: jobResponse{
					NextRecordsURL: "https://test.salesforce.com/services/data/v42.0/jobs/ingest?isPkChunkingEnabled=false&jobType=V2Ingest",
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
			j := &Jobs{
				session:  tt.fields.session,
				response: tt.fields.response,
			}
			got, err := j.Next(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("Jobs.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Jobs.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}
