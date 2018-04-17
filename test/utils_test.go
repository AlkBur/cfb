package test

import (
	"testing"
	"github.com/AlkBur/cfb"
)

func TestInt(t *testing.T)  {
	t.Logf("max uint = %v\n", cfb.MaxUint)
	t.Logf("min uint = %v\n", cfb.MinUint)
	t.Logf("max int = %v\n", cfb.MaxInt)
	t.Logf("min int = %v\n", cfb.MinInt)
}
