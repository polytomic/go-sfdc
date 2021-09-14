package soql

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3/session"
)

func testNewQueryRecords(records []map[string]interface{}) []*QueryRecord {
	recs := make([]*QueryRecord, len(records))
	for idx, record := range records {
		rec, err := newQueryRecord(record, nil)
		if err != nil {
			return nil
		}
		recs[idx] = rec
	}
	return recs
}
func Test_newQueryResult(t *testing.T) {
	type args struct {
		response QueryResponse
	}
	tests := []struct {
		name    string
		args    args
		want    *QueryResult
		wantErr bool
	}{
		{
			name: "No sub results",
			args: args{
				response: QueryResponse{
					Done:      true,
					TotalSize: 2,
					Records: []map[string]interface{}{
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
							},
							"Name": "Test 1",
						},
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
							},
							"Name": "Test 2",
						},
					},
				},
			},
			want: &QueryResult{
				response: QueryResponse{
					Done:      true,
					TotalSize: 2,
					Records: []map[string]interface{}{
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
							},
							"Name": "Test 1",
						},
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
							},
							"Name": "Test 2",
						},
					},
				},
				records: testNewQueryRecords([]map[string]interface{}{
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
						},
						"Name": "Test 1",
					},
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
						},
						"Name": "Test 2",
					},
				}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewQueryResult(tt.args.response, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("newQueryResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newQueryResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryResult_Done(t *testing.T) {
	type fields struct {
		response QueryResponse
		records  []*QueryRecord
		resource *Resource
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "Done",
			fields: fields{
				response: QueryResponse{
					Done: true,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &QueryResult{
				response: tt.fields.response,
				records:  tt.fields.records,
				resource: tt.fields.resource,
			}
			if got := result.Done(); got != tt.want {
				t.Errorf("QueryResult.Done() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryResult_TotalSize(t *testing.T) {
	type fields struct {
		response QueryResponse
		records  []*QueryRecord
		resource *Resource
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "Total Size",
			fields: fields{
				response: QueryResponse{
					TotalSize: 23,
				},
			},
			want: 23,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &QueryResult{
				response: tt.fields.response,
				records:  tt.fields.records,
				resource: tt.fields.resource,
			}
			if got := result.TotalSize(); got != tt.want {
				t.Errorf("QueryResult.TotalSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryResult_MoreRecords(t *testing.T) {
	type fields struct {
		response QueryResponse
		records  []*QueryRecord
		resource *Resource
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "Has More",
			fields: fields{
				response: QueryResponse{
					NextRecordsURL: "The Next URL",
				},
				resource: &Resource{},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &QueryResult{
				response: tt.fields.response,
				records:  tt.fields.records,
				resource: tt.fields.resource,
			}
			if got := result.MoreRecords(); got != tt.want {
				t.Errorf("QueryResult.MoreRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryResult_Records(t *testing.T) {
	type fields struct {
		response QueryResponse
		records  []*QueryRecord
		resource *Resource
	}
	tests := []struct {
		name   string
		fields fields
		want   []*QueryRecord
	}{
		{
			name: "Result Records",
			fields: fields{
				records: testNewQueryRecords([]map[string]interface{}{
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
						},
						"Name": "Test 1",
					},
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
						},
						"Name": "Test 2",
					},
				}),
			},
			want: testNewQueryRecords([]map[string]interface{}{
				{
					"attributes": map[string]interface{}{
						"type": "Account",
						"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
					},
					"Name": "Test 1",
				},
				{
					"attributes": map[string]interface{}{
						"type": "Account",
						"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
					},
					"Name": "Test 2",
				},
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &QueryResult{
				response: tt.fields.response,
				records:  tt.fields.records,
				resource: tt.fields.resource,
			}
			if got := result.Records(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryResult.Records() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryResult_Next(t *testing.T) {
	type fields struct {
		response QueryResponse
		records  []*QueryRecord
		resource *Resource
	}
	tests := []struct {
		name    string
		fields  fields
		want    *QueryResult
		wantErr bool
	}{
		{
			name:    "No more records",
			fields:  fields{},
			want:    nil,
			wantErr: true,
		},
		{
			name: "No more records",
			fields: fields{
				response: QueryResponse{
					NextRecordsURL: "/services/data/v20.0/query/01gD0000002HU6KIAW-2000",
				},
				resource: &Resource{
					session: &session.Mock{
						URL: "https://test.salesforce.com",
						HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
							if req.URL.String() != "https://test.salesforce.com/services/data/v20.0/query/01gD0000002HU6KIAW-2000" {
								return &http.Response{
									StatusCode: 500,
									Status:     "Some Status",
									Body:       ioutil.NopCloser(strings.NewReader("Error")),
									Header:     make(http.Header),
								}
							}
							resp := `
							{
								"done" : true,
								"totalSize" : 2,
								"records" :
								[
									{
										"attributes" :
										{
											"type" : "Account",
											"url" : "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH"
										},
										"Name" : "Test 1"
									},
									{
										"attributes" :
										{
											"type" : "Account",
											"url" : "/services/data/v20.0/sobjects/Account/001D000000IomazIAB"
										},
										"Name" : "Test 2"
									}
								]
							}`

							return &http.Response{
								StatusCode: 200,
								Body:       ioutil.NopCloser(strings.NewReader(resp)),
								Header:     make(http.Header),
							}
						}),
					},
				},
			},
			want: &QueryResult{
				response: QueryResponse{
					Done:      true,
					TotalSize: 2,
					Records: []map[string]interface{}{
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
							},
							"Name": "Test 1",
						},
						{
							"attributes": map[string]interface{}{
								"type": "Account",
								"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
							},
							"Name": "Test 2",
						},
					},
				},
				records: testNewQueryRecords([]map[string]interface{}{
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IRFmaIAH",
						},
						"Name": "Test 1",
					},
					{
						"attributes": map[string]interface{}{
							"type": "Account",
							"url":  "/services/data/v20.0/sobjects/Account/001D000000IomazIAB",
						},
						"Name": "Test 2",
					},
				}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &QueryResult{
				response: tt.fields.response,
				records:  tt.fields.records,
				resource: tt.fields.resource,
			}
			got, err := result.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryResult.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				tt.want.resource = result.resource
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryResult.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}
