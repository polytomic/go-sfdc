package batch

import (
	"encoding/json"
	"errors"

	"github.com/namely/go-sfdc/v3"
)

func HandleSubrequestError(result Subvalue) error {
	errs := sfdc.Errors{}
	err := json.Unmarshal(result.Result, &errs)
	if err != nil {
		return errors.New(string(result.Result))
	}
	return errs
}
