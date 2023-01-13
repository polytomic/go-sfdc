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
					HTTPClient: mockHTTPClient(
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
						}`),
					),
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
					HTTPClient: mockHTTPClient(
						returnStatus(http.StatusOK),
						returnBody(`{
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
						}`),
					),
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
					HTTPClient: mockHTTPClient(
						returnStatus(http.StatusBadRequest),
						returnBody(`[
							{
								"fields" : [ "Id" ],
								"message" : "Account ID: id value of incorrect type: 001900K0001pPuOAAU",
								"errorCode" : "MALFORMED_ID"
							}
						]`),
					),
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
						}`),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query"),
						returnStatus(http.StatusOK),
						returnBody(`{
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
						 }`),
					),
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
						}`),
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
		"Passing -- Query": {
			endpoint: V2QueryEndpoint,
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query"),
						returnStatus(http.StatusOK),
						returnBody(`{
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
						 }`,
						),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234"),
						expectMethod(http.MethodPatch),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234"),
						expectMethod(http.MethodPatch),
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
					HTTPClient: mockHTTPClient(
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
							"systemModstamp": "1/1/1980",
							"apexProcessingTime": 0,
							"apiActiveProcessingTime": 70,
							"numberRecordsFailed": 1,
							"numberRecordsProcessed": 1,
							"retries": 0,
							"totalProcessingTime": 105,
							"errorMessage": ""
						}`,
						),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234"),
						expectMethod(http.MethodGet),
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
							"systemModstamp": "1/1/1980",
							"apexProcessingTime": 0,
							"apiActiveProcessingTime": 70,
							"numberRecordsFailed": 1,
							"numberRecordsProcessed": 1,
							"retries": 0,
							"totalProcessingTime": 105,
							"errorMessage": ""
						}`,
						),
					),
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
					HTTPClient: mockHTTPClient(
						expectMethod(http.MethodGet),
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query/1234"),
						returnStatus(http.StatusOK),
						returnBody(`{
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
						}`,
						),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234"),
						expectMethod(http.MethodDelete),
						returnStatus(http.StatusNoContent),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query/1234"),
						expectMethod(http.MethodDelete),
						returnStatus(http.StatusNoContent),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234"),
						expectMethod(http.MethodDelete),
						returnStatus(http.StatusBadRequest),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/batches"),
						expectMethod(http.MethodPut),
						returnStatus(http.StatusCreated),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/query/1234/results?maxRecords=10000"),
						expectMethod(http.MethodGet),
						expectHeader("Accept", "text/csv"),
						returnStatus(http.StatusOK),
						returnBody("sf__Created|sf__Id|FirstName|LastName|DOB\ntrue|2345|John|Doe|1/1/1970\ntrue|9876|Jane|Doe|1/1/1980\n"),
						returnHeader("Sforce-Locator", "next"),
						returnHeader("Sforce-NumberOfRecords", "4"),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/successfulResults/"),
						expectMethod(http.MethodGet),
						returnStatus(http.StatusOK),
						returnBody("sf__Created|sf__Id|FirstName|LastName|DOB\ntrue|2345|John|Doe|1/1/1970\ntrue|9876|Jane|Doe|1/1/1980\n"),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/failedResults/"),
						expectMethod(http.MethodGet),
						returnStatus(http.StatusOK),
						returnBody("sf__Error|sf__Id|FirstName|LastName|DOB\nREQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --||John|Doe|1/1/1970\nREQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --||Jane|Doe|1/1/1980\n"),
					),
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
					HTTPClient: mockHTTPClient(
						expectURL("https://test.salesforce.com/services/data/v42.0/jobs/ingest/1234/unprocessedrecords/"),
						expectMethod(http.MethodGet),
						returnStatus(http.StatusOK),
						returnBody("FirstName|LastName|DOB\nJohn|Doe|1/1/1970\nJane|Doe|1/1/1980\n"),
					),
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
