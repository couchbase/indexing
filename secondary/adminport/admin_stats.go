package adminport

import (
	c "github.com/couchbase/indexing/secondary/common"
)

func (s *httpServer) newStats() *c.ComponentStat {
	statsp, _ := c.NewComponentStat("adminport", nil)
	stats := *statsp
	stats["componentName"] = s.component + ".adminport"
	stats["urlPrefix"] = s.urlPrefix   // HTTP mount point
	stats["payload"] = []float64{0, 0} // [request,response] Bytes
	for _, msg := range s.messages {   // [req,resp,err] count
		stats["message."+msg.Name()] = []float64{0, 0, 0}
	}
	stats["message.stats"] = []float64{0, 0, 0}
	stats["messages"] = float64(len(s.messages)) // registered messages
	return statsp
}
