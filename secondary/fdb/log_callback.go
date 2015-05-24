package forestdb

import "C"

//export LogCallback
func LogCallback(errCode C.int, msg *C.char, ctx *C.char) {
	Log.Errorf("ForestDB Error (%s) errcode: %d, msg: %s",
		C.GoString(ctx), int(errCode), C.GoString(msg))
}
