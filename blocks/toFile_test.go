package newblocks

import (
	"testing"
)

func TestBlock(t *testing.T) {
	base := Block{}
	b := &ToFile{
		Block: base,
	}
	go BlockRoutine(b)
	t.Errorf("Fail")
}
