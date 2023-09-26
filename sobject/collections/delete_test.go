package collections

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/session"
	"github.com/namely/go-sfdc/v3/sobject"
)

func TestDelete_values(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		allOrNone bool
		records   []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *url.Values
	}{
		{
			name:   "values",
			fields: fields{},
			args: args{
				allOrNone: true,
				records:   []string{"id1", "id2", "id3"},
			},
			want: &url.Values{
				"ids":       []string{"id1,id2,id3"},
				"allOrNone": []string{"true"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &remove{
				session: tt.fields.session,
			}
			if got := d.values(tt.args.allOrNone, tt.args.records); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete.values() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDelete_Callout(t *testing.T) {
	type fields struct {
		session session.ServiceFormatter
	}
	type args struct {
		allOrNone bool
		records   []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []DeleteValue
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				records: []string{"id1", "id2", "id3"},
			},
			fields: fields{
				session: &session.Mock{
					URL: "something.com",
					HTTPClient: mockHTTPClient(func(req *http.Request) *http.Response {
						if strings.HasPrefix(req.URL.String(), "something.com/services/data/v42.0/composite/sobjects") == false {
							return &http.Response{
								StatusCode: 500,
								Status:     "Bad URL: " + req.URL.String(),
								Body:       io.NopCloser(strings.NewReader("resp")),
								Header:     make(http.Header),
							}
						}

						if req.Method != http.MethodDelete {
							return &http.Response{
								StatusCode: 500,
								Status:     "Bad Method",
								Body:       io.NopCloser(strings.NewReader("resp")),
								Header:     make(http.Header),
							}
						}

						values := req.URL.Query()
						if _, ok := values["allOrNone"]; ok == false {

							return &http.Response{
								StatusCode: 500,
								Status:     "allOrNone",
								Body:       io.NopCloser(strings.NewReader("resp")),
								Header:     make(http.Header),
							}
						}
						if _, ok := values["ids"]; ok == false {

							return &http.Response{
								StatusCode: 500,
								Status:     "ids",
								Body:       io.NopCloser(strings.NewReader("resp")),
								Header:     make(http.Header),
							}
						}
						resp := `
						[
							{
								"id" : "001RM000003oLrfYAE",
								"success" : true,
								"errors" : [ ]
							 },
							 {
								"success" : false,
								"errors" : [
								   {
									  "statusCode" : "MALFORMED_ID",
									  "message" : "malformed id 001RM000003oLrB000",
									  "fields" : [ ]
								   }
								]
							 }
						  ]`

						return &http.Response{
							StatusCode: http.StatusOK,
							Status:     "Some Status",
							Body:       io.NopCloser(strings.NewReader(resp)),
							Header:     make(http.Header),
						}
					}),
				},
			},
			want: []DeleteValue{
				{
					sobject.InsertValue{
						Success: true,
						ID:      "001RM000003oLrfYAE",
						Errors:  make([]sfdc.Error, 0),
					},
				},
				{
					sobject.InsertValue{
						Success: false,
						Errors: []sfdc.Error{
							{
								ErrorCode: "MALFORMED_ID",
								Message:   "malformed id 001RM000003oLrB000",
								Fields:    make([]string, 0),
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
			d := &remove{
				session: tt.fields.session,
			}
			got, err := d.callout(context.Background(), tt.args.allOrNone, tt.args.records)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete.Callout() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete.Callout() = %v, want %v", got, tt.want)
			}
		})
	}
}
