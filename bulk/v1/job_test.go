package bulkv1

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"

	"github.com/stretchr/testify/assert"
)

func TestJob_formatOptions(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	type args struct {
		options *Options
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Options
		wantErr bool
	}{
		{
			name:   "passing",
			fields: fields{},
			args: args{
				options: &Options{
					ExternalIDFieldName: "Some External Field",
					Object:              "Account",
					Operation:           bulk.Insert,
					ConcurrencyMode:     bulk.Serial,
					ContentType:         bulk.JSON,
				},
			},
			want: &Options{
				ExternalIDFieldName: "Some External Field",
				Object:              "Account",
				Operation:           bulk.Insert,
				ConcurrencyMode:     bulk.Serial,
				ContentType:         bulk.JSON,
			},
			wantErr: false,
		},
		{
			name:   "defaults",
			fields: fields{},
			args: args{
				options: &Options{
					ExternalIDFieldName: "Some External Field",
					Object:              "Account",
					Operation:           bulk.Insert,
				},
			},
			want: &Options{
				ExternalIDFieldName: "Some External Field",
				Object:              "Account",
				Operation:           bulk.Insert,
				ConcurrencyMode:     bulk.Parallel,
				ContentType:         bulk.JSON,
			},
			wantErr: false,
		},
		{
			name:   "no object",
			fields: fields{},
			args: args{
				options: &Options{
					ExternalIDFieldName: "Some External Field",
					Operation:           bulk.Insert,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "no operation",
			fields: fields{},
			args: args{
				options: &Options{
					ExternalIDFieldName: "Some External Field",
					Object:              "Account",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "no external fields",
			fields: fields{},
			args: args{
				options: &Options{
					Object:    "Account",
					Operation: bulk.Upsert,
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			err := j.formatOptions(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.formatOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && !reflect.DeepEqual(tt.args.options, tt.want) {
				t.Errorf("Job.formatOptions() = %v, want %v", tt.args.options, tt.want)
			}
		})
	}
}

func testNewRequest() *http.Request {
	req, _ := http.NewRequest(http.MethodGet, "https://test.salesforce.com", nil)
	return req
}
func TestJob_response(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	type args struct {
		request *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bulk.Response
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						resp := `{
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
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: bulk.Response{
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
			wantErr: false,
		},
		{
			name: "failing",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						resp := `[
							{
								"fields" : [ "Id" ],
								"message" : "Account ID: id value of incorrect type: 001900K0001pPuOAAU",
								"errorCode" : "MALFORMED_ID"
							}
						]`
						return &http.Response{
							StatusCode: http.StatusBadRequest,
							Status:     "Bad",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want:    bulk.Response{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.response(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.response() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.response() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_createCallout(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bulk.Response
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						resp := `{
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
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				options: Options{
					ExternalIDFieldName: "Some External Field",
					Object:              "Account",
					Operation:           bulk.Insert,
				},
			},
			want: bulk.Response{
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.createCallout(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.createCallout() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.createCallout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_create(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
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
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						resp := `{
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
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				options: Options{
					ExternalIDFieldName: "Some External Field",
					Object:              "Account",
					Operation:           bulk.Insert,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			if err := j.create(tt.args.options); (err != nil) != tt.wantErr {
				t.Errorf("Job.create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_setState(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	type args struct {
		state bulk.State
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bulk.Response
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: bulk.Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodPost {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       ioutil.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}
						resp := `{
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
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				state: bulk.Closed,
			},
			want: bulk.Response{
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
			wantErr: false,
		},
		{
			name: "failing",
			fields: fields{
				info: bulk.Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodPatch {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       ioutil.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}
						resp := `[
							{
								"fields" : [ "Id" ],
								"message" : "Account ID: id value of incorrect type: 001900K0001pPuOAAU",
								"errorCode" : "MALFORMED_ID"
							}
						]`
						return &http.Response{
							StatusCode: http.StatusBadRequest,
							Status:     "Bad",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				state: bulk.Closed,
			},
			want:    bulk.Response{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.setState(tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.setState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.setState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_infoResponse(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	type args struct {
		request *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bulk.Info
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						resp := `{
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
							"systemModstamp": "1/1/1980",
							"apexProcessingTime": 0,
							"apiActiveProcessingTime": 70,
							"numberRecordsFailed": 1,
							"numberRecordsProcessed": 1,
							"retries": 0,
							"totalProcessingTime": 105,
							"errorMessage": ""
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: bulk.Info{
				Response: bulk.Response{
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
				ApexProcessingTime:      0,
				APIActiveProcessingTime: 70,
				NumberRecordsFailed:     1,
				NumberRecordsProcessed:  1,
				Retries:                 0,
				TotalProcessingTime:     105,
				ErrorMessage:            "",
			},
			wantErr: false,
		},
		{
			name: "failing",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						resp := `[
							{
								"fields" : [ "Id" ],
								"message" : "Account ID: id value of incorrect type: 001900K0001pPuOAAU",
								"errorCode" : "MALFORMED_ID"
							}
						]`
						return &http.Response{
							StatusCode: http.StatusBadRequest,
							Status:     "Bad",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want:    bulk.Info{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.infoResponse(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.infoResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.infoResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_Info(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	tests := []struct {
		name    string
		fields  fields
		want    bulk.Info
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: bulk.Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       ioutil.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := `{
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
							"systemModstamp": "1/1/1980",
							"apexProcessingTime": 0,
							"apiActiveProcessingTime": 70,
							"numberRecordsFailed": 1,
							"numberRecordsProcessed": 1,
							"retries": 0,
							"totalProcessingTime": 105,
							"errorMessage": ""
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: bulk.Info{
				Response: bulk.Response{
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
				ApexProcessingTime:      0,
				APIActiveProcessingTime: 70,
				NumberRecordsFailed:     1,
				NumberRecordsProcessed:  1,
				Retries:                 0,
				TotalProcessingTime:     105,
				ErrorMessage:            "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.Info()
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.Info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.Info() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_Batches(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    bulk.Response
	}
	tests := []struct {
		name    string
		fields  fields
		want    *JobBatches
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: bulk.Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job/1234/batch" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       ioutil.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       ioutil.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := `{
							"batchInfo" : [
							   {
								  "apexProcessingTime" : 0,
								  "apiActiveProcessingTime" : 0,
								  "createdDate" : "2015-12-15T21:56:43.000+0000",
								  "id" : "751D00000004YGZIA2",
								  "jobId" : "750D00000004SkVIAU",
								  "numberRecordsFailed" : 0,
								  "numberRecordsProcessed" : 0,
								  "state" : "Queued",
								  "systemModstamp" : "2015-12-15T21:57:19.000+0000",
								  "totalProcessingTime" : 0
							   },
							   {
								  "apexProcessingTime" : 0,
								  "apiActiveProcessingTime" : 2166,
								  "createdDate" : "2015-12-15T22:52:49.000+0000",
								  "id" : "751D00000004YGeIAM",
								  "jobId" : "750D00000004SkVIAU",
								  "numberRecordsFailed" : 0,
								  "numberRecordsProcessed" : 800,
								  "state" : "Completed",
								  "systemModstamp" : "2015-12-15T22:54:54.000+0000",
								  "totalProcessingTime" : 5870
							   }
							]
						 }`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "OK",
							Body:       ioutil.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: &JobBatches{
				job: bulk.Response{ID: "1234"},
				info: []BatchInfo{
					{
						ID:             "751D00000004YGZIA2",
						JobID:          "750D00000004SkVIAU",
						CreatedDate:    "2015-12-15T21:56:43.000+0000",
						State:          Queued,
						SystemModstamp: "2015-12-15T21:57:19.000+0000",
					},
					{
						ID:                      "751D00000004YGeIAM",
						JobID:                   "750D00000004SkVIAU",
						CreatedDate:             "2015-12-15T22:52:49.000+0000",
						State:                   Completed,
						APIActiveProcessingTime: 2166,
						NumberRecordsProcessed:  800,
						SystemModstamp:          "2015-12-15T22:54:54.000+0000",
						TotalProcessingTime:     5870,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want.session = tt.fields.session
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got, err := j.Batches()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
