package resequencer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/resequencer"
)

func TestResequencer(t *testing.T) {
	cases := []struct {
		i []uint64
		o []uint64
	}{
		{[]uint64{}, []uint64{}},
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3}},
		{[]uint64{1, 3, 2, 5, 4}, []uint64{1, 2, 3, 4, 5}},
		{[]uint64{5, 4, 10, 1, 3, 2, 6, 7, 8, 9}, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			r := resequencer.New(len(c.i))
			for _, n := range c.i {
				r.Sequence(n)
			}
			r.Close()
			res := make([]uint64, 0, len(c.o))
			for n := range r.Out {
				res = append(res, n)
			}
			assert.Equal(t, c.o, res)
		})
	}
}
