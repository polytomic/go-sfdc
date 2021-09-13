package batch

import (
	"encoding/json"

	"github.com/namely/go-sfdc/v3"
	"github.com/pkg/errors"
)

func HandleSubrequestError(result Subvalue) error {
	errs := sfdc.Errors{}
	err := json.Unmarshal(result.Result, &errs)
	if err != nil {
		return errors.New(string(result.Result))
	}
	return errs
}
