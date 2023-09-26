package soql

import (
	"errors"
)

type QueryResponse struct {
	Done           bool                     `json:"done"`
	TotalSize      int                      `json:"totalSize"`
	NextRecordsURL string                   `json:"nextRecordsUrl"`
	Records        []map[string]interface{} `json:"records"`
}

func newQueryResponseJSON(jsonMap map[string]interface{}) (QueryResponse, error) {
	response := QueryResponse{}
	if d, has := jsonMap["done"]; has {
		if done, ok := d.(bool); ok {
			response.Done = done
		} else {
			return QueryResponse{}, errors.New("query response: done is not a bool")
		}
	} else {
		return QueryResponse{}, errors.New("query response: done is not present")
	}
	if ts, has := jsonMap["totalSize"]; has {
		if totalSize, ok := ts.(float64); ok {
			response.TotalSize = int(totalSize)
		} else {
			return QueryResponse{}, errors.New("query response: totalSize is not a number")
		}
	} else {
		return QueryResponse{}, errors.New("query response: totalSize is not present")
	}
	if nru, has := jsonMap["nextRecordsUrl"]; has {
		if nextRecordsURL, ok := nru.(string); ok {
			response.NextRecordsURL = nextRecordsURL
		} else {
			return QueryResponse{}, errors.New("query response: nextRecordsUrl is not a string")
		}
	}
	if r, has := jsonMap["records"]; has {
		if array, ok := r.([]interface{}); ok {
			records := make([]map[string]interface{}, len(array))
			for idx, element := range array {
				if rec, ok := element.(map[string]interface{}); ok {
					records[idx] = rec
				} else {
					return QueryResponse{}, errors.New("query response: record element is not a map")
				}
			}
			response.Records = records
		} else {
			return QueryResponse{}, errors.New("query response: records is not an array")
		}
	} else {
		return QueryResponse{}, errors.New("query response: records is not present")
	}
	return response, nil
}

type ColumnMetadata struct {
	Aggregate      bool             `json:"aggregate"`
	ApexType       string           `json:"apexType"`
	BooleanType    bool             `json:"booleanType"`
	ColumnName     string           `json:"columnName"`
	Custom         bool             `json:"custom"`
	DisplayName    string           `json:"displayName"`
	ForeignKeyName *string          `json:"foreignKeyName"`
	Insertable     bool             `json:"insertable"`
	JoinColumns    []ColumnMetadata `json:"joinColumns"`
	NumberType     bool             `json:"numberType"`
	TextType       bool             `json:"textType"`
	Updateable     bool             `json:"updateable"`
}
type QueryColumnMetadataResponse struct {
	EntityName     string           `json:"entityName"`
	GroupBy        bool             `json:"groupBy"`
	IdSelected     bool             `json:"idSelected"`
	KeyPrefix      string           `json:"keyPrefix"`
	ColumnMetadata []ColumnMetadata `json:"columnMetadata"`
}
