package bulkv1

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3/bulk"
	"github.com/namely/go-sfdc/v3/session"
	"github.com/stretchr/testify/assert"
)

func TestBatch_response(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		request *http.Request
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BatchInfo
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					HTTPClient: mockHTTPClient(
						func(req *http.Request) *http.Response {
							resp := `{
							"apexProcessingTime" : 0,
							"apiActiveProcessingTime" : 0,
							"createdDate" : "2015-12-15T22:52:49.000+0000",
							"id" : "751D00000004YGeIAM",
							"jobId" : "750D00000004SkVIAU",
							"numberRecordsFailed" : 0,
							"numberRecordsProcessed" : 0,
							"state" : "InProgress",
							"systemModstamp" : "2015-12-15T22:52:49.000+0000",
							"totalProcessingTime" : 0
						}`
							return &http.Response{
								StatusCode: http.StatusOK,
								Status:     "OK",
								Body:       io.NopCloser(strings.NewReader(resp)),
								Header:     make(http.Header),
							}
						},
					),
				},
			},
			args: args{
				request: testNewRequest(),
			},
			want: BatchInfo{
				ID:             "751D00000004YGeIAM",
				JobID:          "750D00000004SkVIAU",
				State:          InProgress,
				CreatedDate:    "2015-12-15T22:52:49.000+0000",
				SystemModstamp: "2015-12-15T22:52:49.000+0000",
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
			want:    BatchInfo{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Batch{
				session: tt.fields.session,
			}
			got, err := j.infoResponse(tt.args.request)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBatch_fetchInfo(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		id    string
		jobID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BatchInfo
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						func(req *http.Request) *http.Response {
							resp := `{
							"apexProcessingTime" : 0,
							"apiActiveProcessingTime" : 0,
							"createdDate" : "2015-12-15T22:52:49.000+0000",
							"id" : "abcd",
							"jobId" : "1234",
							"numberRecordsFailed" : 0,
							"numberRecordsProcessed" : 0,
							"state" : "InProgress",
							"systemModstamp" : "2015-12-15T22:52:49.000+0000",
							"totalProcessingTime" : 0
						}`
							return &http.Response{
								StatusCode: http.StatusOK,
								Status:     "OK",
								Body:       io.NopCloser(strings.NewReader(resp)),
								Header:     make(http.Header),
							}
						},
						wantMethod(http.MethodGet),
						wantURL("https://test.salesforce.com/services/async/42.0/job/1234/batch/abcd"),
					),
				},
			},
			args: args{
				id:    "abcd",
				jobID: "1234",
			},
			want: BatchInfo{
				ID:             "abcd",
				JobID:          "1234",
				State:          InProgress,
				CreatedDate:    "2015-12-15T22:52:49.000+0000",
				SystemModstamp: "2015-12-15T22:52:49.000+0000",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				session: tt.fields.session,
			}
			err := b.fetchInfo(context.Background(), tt.args.jobID, tt.args.id)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, b.Info)
		})
	}
}

func TestBatch_create(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		jobID       string
		contentType bulk.ContentType
		body        string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BatchInfo
		wantErr bool
	}{
		{
			name: "Passing",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						func(req *http.Request) *http.Response {
							resp := `{
							"apexProcessingTime" : 0,
							"apiActiveProcessingTime" : 0,
							"createdDate" : "2015-12-15T22:52:49.000+0000",
							"id" : "abcd",
							"jobId" : "1234",
							"numberRecordsFailed" : 0,
							"numberRecordsProcessed" : 0,
							"state" : "Queued",
							"systemModstamp" : "2015-12-15T22:52:49.000+0000",
							"totalProcessingTime" : 0
						}`
							return &http.Response{
								StatusCode: http.StatusCreated,
								Status:     "OK",
								Body:       io.NopCloser(strings.NewReader(resp)),
								Header:     make(http.Header),
							}
						},
						wantMethod(http.MethodPost),
						wantURL("https://test.salesforce.com/services/async/42.0/job/1234/batch"),
					),
				},
			},
			args: args{
				jobID: "1234",
			},
			want: BatchInfo{
				ID:             "abcd",
				JobID:          "1234",
				State:          Queued,
				CreatedDate:    "2015-12-15T22:52:49.000+0000",
				SystemModstamp: "2015-12-15T22:52:49.000+0000",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				session: tt.fields.session,
			}
			err := b.create(context.Background(), tt.args.jobID, tt.args.contentType, strings.NewReader(tt.args.body))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, b.Info)
		})
	}
}

func TestBatch_Results(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		info BatchInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BatchResult
		wantErr bool
	}{
		{
			name: "All successful",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: multiMockHTTPClient(
						map[string]mock{
							"https://test.salesforce.com/services/async/42.0/job/1234/batch/abcd/result": {
								fn: func(req *http.Request) *http.Response {
									resp := `[
		{
		   "success" : true,
		   "created" : true,
		   "id" : "001xx000003DHP0AAO",
		   "errors" : []
		},
		{
		   "success" : true,
		   "created" : true,
		   "id" : "001xx000003DHP1AAO",
		   "errors" : []
		}
	 ]`
									return &http.Response{
										StatusCode: http.StatusOK,
										Status:     "OK",
										Body:       io.NopCloser(strings.NewReader(resp)),
										Header:     make(http.Header),
									}
								},
								cond: []mockHTTPFilter{
									wantMethod(http.MethodGet),
								},
							},
							"https://test.salesforce.com/services/async/42.0/job/1234/batch/abcd/request": {
								fn: func(req *http.Request) *http.Response {
									resp := `[]`
									return &http.Response{
										StatusCode: http.StatusOK,
										Status:     "OK",
										Body:       io.NopCloser(strings.NewReader(resp)),
										Header:     make(http.Header),
									}
								},
								cond: []mockHTTPFilter{
									wantMethod(http.MethodGet),
								},
							},
						},
					),
				},
			},
			args: args{
				info: BatchInfo{
					ID:    "abcd",
					JobID: "1234",
				},
			},
			want: BatchResult{
				Successful: []bulk.SuccessfulRecord{
					{
						Created: true,
						JobRecord: bulk.JobRecord{
							ID: "001xx000003DHP0AAO",
							UnprocessedRecord: bulk.UnprocessedRecord{
								Fields: map[string]interface{}{},
							},
						},
					},
					{
						Created: true,
						JobRecord: bulk.JobRecord{
							ID: "001xx000003DHP1AAO",
							UnprocessedRecord: bulk.UnprocessedRecord{
								Fields: map[string]interface{}{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "With failed records",
			fields: fields{
				session: &session.Mock{
					URL: "https://test.salesforce.com",
					HTTPClient: mockHTTPClient(
						func(req *http.Request) *http.Response {
							var resp string
							switch req.URL.String() {
							case "https://test.salesforce.com/services/async/42.0/job/1234/batch/abcd/result":
								resp = `[
								{
								   "success" : true,
								   "created" : true,
								   "id" : "001xx000003DHP0AAO",
								   "errors" : []
								},
								{
								   "success" : false,
								   "created" : false,
								   "id" : "001xx000003DHP1AAO",
								   "errors" : [{"message": "REQUIRED_FIELD_MISSING:Required fields are missing: [LastName]:LastName --"}]
								}
							 ]`
							case "https://test.salesforce.com/services/async/42.0/job/1234/batch/abcd/request":
								resp = `[]`
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Status:     "OK",
								Body:       io.NopCloser(strings.NewReader(resp)),
								Header:     make(http.Header),
							}
						},
						wantMethod(http.MethodGet),
					),
				},
			},
			args: args{
				info: BatchInfo{
					ID:    "abcd",
					JobID: "1234",
				},
			},
			want: BatchResult{
				Successful: []bulk.SuccessfulRecord{
					{
						Created: true,
						JobRecord: bulk.JobRecord{
							ID: "001xx000003DHP0AAO",
							UnprocessedRecord: bulk.UnprocessedRecord{
								Fields: map[string]interface{}{},
							},
						},
					},
				},
				Failed: []bulk.FailedRecord{
					{
						Error: "REQUIRED_FIELD_MISSING:Required fields are missing: [LastName]:LastName -- ()",
						JobRecord: bulk.JobRecord{
							ID: "001xx000003DHP1AAO",
							UnprocessedRecord: bulk.UnprocessedRecord{
								Fields: map[string]interface{}{},
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
			b := &Batch{
				session: tt.fields.session,
				Info:    tt.args.info,
			}

			result, err := b.Results(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, result)
		})
	}
}
