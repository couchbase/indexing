package forestdb

import "C"
import "fmt"

//export LogCallback
func LogCallback(errCode C.int, msg *C.char, ctx *C.char) {
	Log.LazyDebug(func() string {
		return fmt.Sprintf("ForestDB Error (%s) errcode: %d, msg: %s", C.GoString(ctx),
			int(errCode), C.GoString(msg))
	})
}
