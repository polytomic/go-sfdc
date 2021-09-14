package bulkv1

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"
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
				session: &session.Mock{},
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
			got, err := NewResource(tt.args.session)
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
			r := &Resource{
				session: tt.fields.session,
			}
			_, err := r.CreateJob(tt.args.options)
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
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if req.URL.String() != "https://test.salesforce.com/services/async/42.0/job/123" {
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Resource{
				session: tt.fields.session,
			}
			_, err := r.GetJob("123")
			if (err != nil) != tt.wantErr {
				t.Errorf("Resource.GetJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
