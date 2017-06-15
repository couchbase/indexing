package datautility

import (
	"fmt"
	json "github.com/couchbase/indexing/secondary/common/json"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

func LoadJSONFromCompressedFile(path, docidfield string) tc.KeyValues {
	file, err := tc.ReadCompressedFile(path)
	tc.HandleError(err, "Error while decompressing data file "+path)

	var data interface{}
	json.Unmarshal(file, &data)

	m := data.([]interface{})
	keyValues := make(tc.KeyValues)

	var i = 0
	var k string
	if len(docidfield) > 0 {
		for _, v := range m {
			k = fmt.Sprintf("%v", v.(map[string]interface{})[docidfield])
			keyValues[k] = v
			i++
		}
	} else {
		for _, v := range m {
			k = fmt.Sprintf("%v", i)
			keyValues[k] = v
			i++
		}
	}

	return keyValues
}
