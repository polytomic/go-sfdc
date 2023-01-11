package bulk

import (
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
	"github.com/stretchr/testify/assert"
)

func TestJob_formatOptions(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
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
					ColumnDelimiter:     Caret,
					ContentType:         CSV,
					ExternalIDFieldName: "Some External Field",
					LineEnding:          Linefeed,
					Object:              "Account",
					Operation:           Insert,
				},
			},
			want: &Options{
				ColumnDelimiter:     Caret,
				ContentType:         CSV,
				ExternalIDFieldName: "Some External Field",
				LineEnding:          Linefeed,
				Object:              "Account",
				Operation:           Insert,
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
					Operation:           Insert,
				},
			},
			want: &Options{
				ColumnDelimiter:     Comma,
				ContentType:         CSV,
				ExternalIDFieldName: "Some External Field",
				LineEnding:          Linefeed,
				Object:              "Account",
				Operation:           Insert,
			},
			wantErr: false,
		},
		{
			name:   "no object",
			fields: fields{},
			args: args{
				options: &Options{
					ExternalIDFieldName: "Some External Field",
					Operation:           Insert,
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
					Operation: Upsert,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:   "passing -- query",
			fields: fields{},
			args: args{
				options: &Options{
					ColumnDelimiter: Caret,
					ContentType:     CSV,
					LineEnding:      Linefeed,
					Operation:       Query,
					Query:           "SELECT Id FROM Account",
				},
			},
			want: &Options{
				ColumnDelimiter: Caret,
				ContentType:     CSV,
				LineEnding:      Linefeed,
				Operation:       Query,
				Query:           "SELECT Id FROM Account",
			},
			wantErr: false,
		},
		{
			name:   "defaults -- query",
			fields: fields{},
			args: args{
				options: &Options{
					Operation: Query,
					Query:     "SELECT Id FROM Account",
				},
			},
			want: &Options{
				ColumnDelimiter: Comma,
				ContentType:     CSV,
				LineEnding:      Linefeed,
				Operation:       Query,
				Query:           "SELECT Id FROM Account",
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

func TestJob_delimiter(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	tests := []struct {
		name   string
		fields fields
		want   rune
	}{
		{
			name: "tab",
			fields: fields{
				info: Response{
					ColumnDelimiter: "TAB",
				},
			},
			want: '\t',
		},
		{
			name: "back quote",
			fields: fields{
				info: Response{
					ColumnDelimiter: "BACKQUOTE",
				},
			},
			want: '`',
		},
		{
			name: "caret",
			fields: fields{
				info: Response{
					ColumnDelimiter: "CARET",
				},
			},
			want: '^',
		},
		{
			name: "comma",
			fields: fields{
				info: Response{
					ColumnDelimiter: "COMMA",
				},
			},
			want: ',',
		},
		{
			name: "pipe",
			fields: fields{
				info: Response{
					ColumnDelimiter: "PIPE",
				},
			},
			want: '|',
		},
		{
			name: "semi colon",
			fields: fields{
				info: Response{
					ColumnDelimiter: "SEMICOLON",
				},
			},
			want: ';',
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			if got := j.delimiter(); got != tt.want {
				t.Errorf("Job.delimiter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_record(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	type args struct {
		fields []string
		values []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]interface{}
	}{
		{
			name:   "make record",
			fields: fields{},
			args: args{
				fields: []string{
					"first",
					"last",
					"DOB",
				},
				values: []string{
					"john",
					"doe",
					"1/1/1970",
				},
			},
			want: map[string]interface{}{
				"first": "john",
				"last":  "doe",
				"DOB":   "1/1/1970",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			if got := j.record(tt.args.fields, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.record() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_fields(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	type args struct {
		columns []string
		offset  int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name:   "passing",
			fields: fields{},
			args: args{
				columns: []string{"sf_id", "first", "last", "DOB"},
				offset:  1,
			},
			want: []string{
				"first",
				"last",
				"DOB",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				session: tt.fields.session,
				info:    tt.fields.info,
			}
			got := j.fields(tt.args.columns, tt.args.offset)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.fields() = %v, want %v", got, tt.want)
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
		info    Response
	}
	type args struct {
		request *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Response
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: Response{
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
			name: "Passing -- query",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						resp := `{
							"apiVersion": 44.0,
							"columnDelimiter": "COMMA",
							"concurrencyMode": "Parallel",
							"contentType": "CSV",
							"createdById": "1234",
							"createdDate": "1/1/1970",
							"id": "9876",
							"lineEnding": "LF",
							"object": "Account",
							"operation": "query",
							"state": "UploadComplete"
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: Response{
				APIVersion:      44.0,
				ColumnDelimiter: "COMMA",
				ConcurrencyMode: "Parallel",
				ContentType:     "CSV",
				CreatedByID:     "1234",
				CreatedDate:     "1/1/1970",
				ID:              "9876",
				LineEnding:      "LF",
				Object:          "Account",
				Operation:       Query,
				State:           "UploadComplete",
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want:    Response{},
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
		info    Response
	}
	type args struct {
		options Options
	}
	tests := map[string]struct {
		endpoint BulkEndpoint
		fields   fields
		args     args
		want     Response
		wantErr  bool
	}{
		"Passing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
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
			want: Response{
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
		"Passing -- Query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/query" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						resp := `{
							"id" : "750R0000000zlh9IAA",
							"operation" : "query",
							"object" : "Account",
							"createdById" : "005R0000000GiwjIAC",
							"createdDate" : "2018-12-10T17:50:19.000+0000",
							"systemModstamp" : "2018-12-10T17:50:19.000+0000",
							"state" : "UploadComplete",
							"concurrencyMode" : "Parallel",
							"contentType" : "CSV",
							"apiVersion" : 46.0,
							"lineEnding" : "LF",
							"columnDelimiter" : "COMMA"
						 }`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				options: Options{
					ColumnDelimiter: Comma,
					ContentType:     CSV,
					LineEnding:      Linefeed,
					Operation:       Query,
				},
			},
			want: Response{
				APIVersion:      46.0,
				ColumnDelimiter: "COMMA",
				ConcurrencyMode: "Parallel",
				ContentType:     "CSV",
				CreatedByID:     "005R0000000GiwjIAC",
				CreatedDate:     "2018-12-10T17:50:19.000+0000",
				LineEnding:      "LF",
				ID:              "750R0000000zlh9IAA",
				Object:          "Account",
				Operation:       "query",
				State:           "UploadComplete",
				SystemModstamp:  "2018-12-10T17:50:19.000+0000",
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j := NewJob(tt.endpoint, tt.fields.session)
			j.info = tt.fields.info

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
		info    Response
	}
	type args struct {
		options Options
	}
	tests := map[string]struct {
		endpoint BulkEndpoint
		fields   fields
		args     args
		wantErr  bool
	}{
		"Passing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
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
		"Passing -- Query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/query" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						resp := `{
							"id" : "750R0000000zlh9IAA",
							"operation" : "query",
							"object" : "Account",
							"createdById" : "005R0000000GiwjIAC",
							"createdDate" : "2018-12-10T17:50:19.000+0000",
							"systemModstamp" : "2018-12-10T17:50:19.000+0000",
							"state" : "UploadComplete",
							"concurrencyMode" : "Parallel",
							"contentType" : "CSV",
							"apiVersion" : 46.0,
							"lineEnding" : "LF",
							"columnDelimiter" : "COMMA"
						 }`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				options: Options{
					ColumnDelimiter: Comma,
					ContentType:     CSV,
					LineEnding:      Linefeed,
					Operation:       Query,
					Query:           "SELECT Id FROM Account",
				},
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j := NewJob(tt.endpoint, tt.fields.session)
			j.info = tt.fields.info

			if err := j.create(tt.args.options); (err != nil) != tt.wantErr {
				t.Errorf("Job.create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_setState(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	type args struct {
		state State
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Response
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodPatch {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				state: UpdateComplete,
			},
			want: Response{
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
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodPatch {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				state: UpdateComplete,
			},
			want:    Response{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob(V2IngestEndpoint, tt.fields.session)
			j.info = tt.fields.info

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
		info    Response
	}
	type args struct {
		request *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Info
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: Info{
				Response: Response{
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want:    Info{},
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
		info    Response
	}
	tests := map[string]struct {
		endpoint BulkEndpoint
		fields   fields
		want     Info
		wantErr  bool
	}{
		"Passing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
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
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: Info{
				Response: Response{
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
		"Passing -- Query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/query/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := `{
							"id" : "750R0000000zlh9IAA",
							"operation" : "query",
							"object" : "Account",
							"createdById" : "005R0000000GiwjIAC",
							"createdDate" : "2018-12-10T17:50:19.000+0000",
							"systemModstamp" : "2018-12-10T17:51:27.000+0000",
							"state" : "JobComplete",
							"concurrencyMode" : "Parallel",
							"contentType" : "CSV",
							"apiVersion" : 46.0,
							"jobType" : "V2Query",
							"lineEnding" : "LF",
							"columnDelimiter" : "COMMA",
							"numberRecordsProcessed" : 500,
							"retries" : 0,
							"totalProcessingTime" : 334
						}`
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: Info{
				Response: Response{
					APIVersion:      46.0,
					ColumnDelimiter: "COMMA",
					ConcurrencyMode: "Parallel",
					ContentType:     "CSV",
					CreatedByID:     "005R0000000GiwjIAC",
					CreatedDate:     "2018-12-10T17:50:19.000+0000",
					ID:              "750R0000000zlh9IAA",
					JobType:         "V2Query",
					LineEnding:      "LF",
					Object:          "Account",
					Operation:       "query",
					State:           "JobComplete",
					SystemModstamp:  "2018-12-10T17:51:27.000+0000",
				},
				NumberRecordsProcessed: 500,
				Retries:                0,
				TotalProcessingTime:    334,
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j := NewJob(tt.endpoint, tt.fields.session)
			j.info = tt.fields.info

			got, err := j.Info()
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.Info() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestJob_Delete(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	tests := map[string]struct {
		endpoint BulkEndpoint
		fields   fields
		wantErr  bool
	}{
		"Passing": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodDelete {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						return &http.Response{
							StatusCode: http.StatusNoContent,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader("")),
							Header:     make(http.Header),
						}

					}),
				},
			},
			wantErr: false,
		},
		"Passing -- Query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/query/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodDelete {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						return &http.Response{
							StatusCode: http.StatusNoContent,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader("")),
							Header:     make(http.Header),
						}

					}),
				},
			},
			wantErr: false,
		},
		"Fail": {
			endpoint: V2IngestEndpoint,
			fields: fields{
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodDelete {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						return &http.Response{
							StatusCode: http.StatusBadRequest,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader("")),
							Header:     make(http.Header),
						}

					}),
				},
			},
			wantErr: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j := NewJob(tt.endpoint, tt.fields.session)
			j.info = tt.fields.info

			if err := j.Delete(); (err != nil) != tt.wantErr {
				t.Errorf("Job.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Upload(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	type args struct {
		body io.Reader
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
				info: Response{
					ID: "1234",
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/batches" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodPut {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						return &http.Response{
							StatusCode: http.StatusCreated,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader("")),
							Header:     make(http.Header),
						}

					}),
				},
			},
			args: args{
				body: strings.NewReader("some reader"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob(V2IngestEndpoint, tt.fields.session)
			j.info = tt.fields.info

			if err := j.Upload(tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("Job.Upload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJob_Results(t *testing.T) {
	type fields struct {
		endpoint   BulkEndpoint
		session    session.ServiceFormatter
		info       Response
		locator    string
		maxRecords int
	}
	tests := map[string]struct {
		fields  fields
		want    ResultsPage
		wantErr bool
	}{
		"Records for ingest job returns error": {
			fields: fields{
				endpoint: V2IngestEndpoint,
				session:  &session.Mock{},
			},
			wantErr: true,
		},
		"Passing": {
			fields: fields{
				endpoint: V2QueryEndpoint,
				info: Response{
					ID:              "1234",
					ColumnDelimiter: Pipe,
					LineEnding:      Linefeed,
				},
				maxRecords: 10000,
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/query/1234/results?maxRecords=10000" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						if req.Header.Get("Accept") != "text/csv" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Accept Header",
								Body:       io.NopCloser(strings.NewReader(req.Header.Get("Accept"))),
								Header:     make(http.Header),
							}
						}

						resp := "sf__Created|sf__Id|FirstName|LastName|DOB\ntrue|2345|John|Doe|1/1/1970\ntrue|9876|Jane|Doe|1/1/1980\n"
						respHeader := make(http.Header)
						respHeader.Set(HeaderLocator, "next")
						respHeader.Set(HeaderRecordCount, "4")
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     respHeader,
						}

					}),
				},
			},
			want: ResultsPage{
				Locator: "next",
				Records: []map[string]interface{}{
					{
						"sf__Created": "true",
						"sf__Id":      "2345",
						"FirstName":   "John",
						"LastName":    "Doe",
						"DOB":         "1/1/1970",
					},
					{
						"sf__Created": "true",
						"sf__Id":      "9876",
						"FirstName":   "Jane",
						"LastName":    "Doe",
						"DOB":         "1/1/1980",
					},
				},
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			j := NewJob(tt.fields.endpoint, tt.fields.session)
			j.info = tt.fields.info

			got, err := j.Results(tt.fields.locator, tt.fields.maxRecords)
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.SuccessfulRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.Equal(t, tt.want, *got)
			}
		})
	}
}

func TestJob_SuccessfulRecords(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	tests := []struct {
		name    string
		fields  fields
		want    []SuccessfulRecord
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: Response{
					ID:              "1234",
					ColumnDelimiter: Pipe,
					LineEnding:      Linefeed,
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/successfulResults/" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := "sf__Created|sf__Id|FirstName|LastName|DOB\ntrue|2345|John|Doe|1/1/1970\ntrue|9876|Jane|Doe|1/1/1980\n"
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: []SuccessfulRecord{
				{
					Created: true,
					JobRecord: JobRecord{
						ID: "2345",
						UnprocessedRecord: UnprocessedRecord{
							Fields: map[string]interface{}{
								"FirstName": "John",
								"LastName":  "Doe",
								"DOB":       "1/1/1970",
							},
						},
					},
				},
				{
					Created: true,
					JobRecord: JobRecord{
						ID: "9876",
						UnprocessedRecord: UnprocessedRecord{
							Fields: map[string]interface{}{
								"FirstName": "Jane",
								"LastName":  "Doe",
								"DOB":       "1/1/1980",
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob(V2IngestEndpoint, tt.fields.session)
			j.info = tt.fields.info

			got, err := j.SuccessfulRecords()
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.SuccessfulRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.SuccessfulRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_FailedRecords(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	tests := []struct {
		name    string
		fields  fields
		want    []FailedRecord
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: Response{
					ID:              "1234",
					ColumnDelimiter: Pipe,
					LineEnding:      Linefeed,
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/failedResults/" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := "\"sf__Error\"|\"sf__Id\"|FirstName|LastName|DOB\nREQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --||John|Doe|1/1/1970\nREQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --||Jane|Doe|1/1/1980\n"
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: []FailedRecord{
				{
					Error: "REQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --",
					JobRecord: JobRecord{
						UnprocessedRecord: UnprocessedRecord{
							Fields: map[string]interface{}{
								"FirstName": "John",
								"LastName":  "Doe",
								"DOB":       "1/1/1970",
							},
						},
					},
				},
				{
					Error: "REQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --",
					JobRecord: JobRecord{
						UnprocessedRecord: UnprocessedRecord{
							Fields: map[string]interface{}{
								"FirstName": "Jane",
								"LastName":  "Doe",
								"DOB":       "1/1/1980",
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob(V2IngestEndpoint, tt.fields.session)
			j.info = tt.fields.info

			got, err := j.FailedRecords()
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.FailedRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.FailedRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_UnprocessedRecords(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
		info    Response
	}
	tests := []struct {
		name    string
		fields  fields
		want    []UnprocessedRecord
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				info: Response{
					ID:              "1234",
					ColumnDelimiter: Pipe,
					LineEnding:      Linefeed,
				},
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/unprocessedrecords/" {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid URL",
								Body:       io.NopCloser(strings.NewReader(req.URL.String())),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodGet {
							return &http.Response{
								StatusCode: 500,
								Status:     "Invalid Method",
								Body:       io.NopCloser(strings.NewReader(req.Method)),
								Header:     make(http.Header),
							}
						}

						resp := "FirstName|LastName|DOB\nJohn|Doe|1/1/1970\nJane|Doe|1/1/1980\n"
						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Good",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}

					}),
				},
			},
			want: []UnprocessedRecord{
				{
					Fields: map[string]interface{}{
						"FirstName": "John",
						"LastName":  "Doe",
						"DOB":       "1/1/1970",
					},
				},
				{
					Fields: map[string]interface{}{
						"FirstName": "Jane",
						"LastName":  "Doe",
						"DOB":       "1/1/1980",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob(V2IngestEndpoint, tt.fields.session)
			j.info = tt.fields.info

			got, err := j.UnprocessedRecords()
			if (err != nil) != tt.wantErr {
				t.Errorf("Job.UnprocessedRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.UnprocessedRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}
