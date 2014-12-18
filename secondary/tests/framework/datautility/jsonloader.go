package datautility

import (
	"encoding/json"
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
)

func LoadJSONFromCompressedFile(path, docidfield string) []kvutility.KeyValue {
	file, err := tc.ReadCompressedFile(path)
	tc.HandleError(err, "Error while decompressing data file "+path)

	var data interface{}
	json.Unmarshal(file, &data)

	m := data.([]interface{})

	keyValues := make([]kvutility.KeyValue, len(m))

	var i = 0
	if len(docidfield) > 0 {
		for _, v := range m {
			keyValues[i].Key = fmt.Sprintf("%v", v.(map[string]interface{})[docidfield])
			keyValues[i].JsonValue = v.(map[string]interface{})
			i++
		}
	} else {
		for _, v := range m {
			keyValues[i].Key = fmt.Sprintf("%v", i)
			keyValues[i].JsonValue = v.(map[string]interface{})
			i++
		}
	}

	return keyValues
}
