package bulk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFormatter(t *testing.T) {
	type args struct {
		job    *Job
		fields []string
	}
	tests := []struct {
		name    string
		args    args
		want    *Formatter
		wantErr bool
	}{
		{
			name: "Passing",
			args: args{
				job: &Job{
					info: Response{
						ColumnDelimiter: Pipe,
						LineEnding:      Linefeed,
					},
				},
				fields: []string{
					"Name",
					"Site",
				},
			},
			want: &Formatter{
				job: &Job{
					info: Response{
						ColumnDelimiter: Pipe,
						LineEnding:      Linefeed,
					},
				},
				fields: []string{
					"Name",
					"Site",
				},
				sb: &strings.Builder{},
			},
			wantErr: false,
		},
		{
			name: "No Job",
			args: args{
				job: nil,
				fields: []string{
					"Name",
					"Site",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "No Fields",
			args: args{
				job: &Job{
					info: Response{
						ColumnDelimiter: Pipe,
						LineEnding:      Linefeed,
					},
				},
				fields: nil,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFormatter(tt.args.job, tt.args.fields)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFormatter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.want != nil {
				tt.want.sb.WriteString(strings.Join(tt.want.fields, string(tt.want.job.Delimiter())))
				tt.want.sb.WriteString("\n")

				assert.Equalf(t, tt.want.job, got.job, "NewFormatter().job = %v, want %v", got.job, tt.want.job)
				assert.Equalf(t, tt.want.fields, got.fields, "NewFormatter.fields = %v, want %v", got.fields, tt.want.fields)
				assert.Equalf(t, tt.want.sb, got.sb, "NewFormatter.sb = %v, want %v", got.sb, tt.want.sb)
			}
		})
	}
}

type testRecord struct {
	fields     map[string]interface{}
	insertNull bool
}

func (t *testRecord) Fields() map[string]interface{} {
	return t.fields
}
func (t *testRecord) InsertNull() bool {
	return t.insertNull
}
func TestFormatter_Add(t *testing.T) {
	type fields struct {
		job        *Job
		fields     []string
		insertNull bool
		sb         *strings.Builder
	}
	type args struct {
		records []Record
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Adding",
			fields: fields{
				job: &Job{
					info: Response{
						ColumnDelimiter: Pipe,
						LineEnding:      Linefeed,
					},
				},
				fields: []string{
					"Name",
					"Site",
				},
				insertNull: true,
				sb:         &strings.Builder{},
			},
			args: args{
				records: []Record{
					&testRecord{
						fields: map[string]interface{}{
							"Name": "name 1",
							"Site": "good site",
						},
					},
					&testRecord{
						fields: map[string]interface{}{
							"Name": "name 2",
							"Site": "great site",
						},
					},
				},
			},
			want:    "Name|Site\nname 1|good site\nname 2|great site\n",
			wantErr: false,
		},
		{
			name: "Adding",
			fields: fields{
				job: &Job{
					info: Response{
						ColumnDelimiter: Pipe,
						LineEnding:      Linefeed,
					},
				},
				fields: []string{
					"Name",
					"Site",
				},
				insertNull: true,
				sb:         &strings.Builder{},
			},
			args: args{
				records: nil,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := NewFormatter(tt.fields.job, tt.fields.fields)
			var err error
			if err = f.Add(tt.args.records...); (err != nil) != tt.wantErr {
				t.Errorf("Formatter.Add() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if tt.want != f.sb.String() {
					t.Errorf("Formatter.Add() want = %v, got %v", tt.want, f.sb.String())
				}
			}
		})
	}
}
