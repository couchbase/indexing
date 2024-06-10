package indexer

import (
	c "github.com/couchbase/indexing/secondary/common"
	p "github.com/couchbase/indexing/secondary/pipeline"
)

type IndexScanSource2 struct {
	p.ItemWriter
	is IndexSnapshot
	p  *ScanPipeline

	parllelCIDScans int
}

func (s *IndexScanSource2) Routine() error {
	// VECTOR_TODO: Implement pipeline for vector scans
	return nil
}

func NewScanPipeline2(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot, cfg c.Config) *ScanPipeline {

	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req
	scanPipeline.config = cfg

	src := &IndexScanSource2{is: is, p: scanPipeline}
	src.InitWriter()
	dec := &IndexScanDecoder{p: scanPipeline}
	dec.InitReadWriter()
	wr := &IndexScanWriter{w: w, p: scanPipeline}
	wr.InitReader()

	dec.SetSource(src)
	wr.SetSource(dec)

	scanPipeline.src = src
	scanPipeline.object.AddSource("source", src)
	scanPipeline.object.AddFilter("decoder", dec)
	scanPipeline.object.AddSink("writer", wr)

	return scanPipeline
}
