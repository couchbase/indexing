package stats

import "time"
import "fmt"

type TimingStat struct {
	Count   Int64Val
	Sum     Int64Val
	SumOfSq Int64Val
	bitmap  uint64
}

func (t *TimingStat) Init() {
	t.Count.Init()
	t.Sum.Init()
	t.SumOfSq.Init()

	// Set the default value of filter bitmap to AllStatsFilter
	t.bitmap = AllStatsFilter
}

func (t *TimingStat) Put(dur time.Duration) {
	t.Count.Add(1)
	t.Sum.Add(int64(dur))
	t.SumOfSq.Add(int64(dur * dur))
}

func (t TimingStat) Value() string {
	return fmt.Sprintf("%d %d %d", t.Count.Value(), t.Sum.Value(), t.SumOfSq.Value())
}

func (t *TimingStat) Map(bitmap uint64) bool {
	return (t.bitmap & bitmap) != 0
}

func (t *TimingStat) AddFilter(filter uint64) {
	t.bitmap |= filter
}

func (t *TimingStat) GetValue() interface{} {
	return fmt.Sprintf("%d %d %d", t.Count.Value(), t.Sum.Value(), t.SumOfSq.Value())
}
