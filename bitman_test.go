package finnishtable

import "testing"

func TestFindHashes(t *testing.T) {
	for i := 0; i < 256; i++ {
		b := uint8(0)
		if isMarkerTophash(b) {
			continue
		}

		meta := new(bucketmeta)
		for j := range meta.tophash8 {
			meta.tophash8[j] = b
			meta.triehash8[j] = b
		}

		tophashProbe, triehashProbe := makeHashProbes(b, b)

		finder := meta.Finder()
		present, _ := finder.ProbeHashMatchesAndEmpties(tophashProbe, triehashProbe)
		if present.Count() != uint8(len(meta.tophash8)) {
			t.Fatalf("did not get present for %v", b)
		}
	}
}

func TestEmpty(t *testing.T) {
	for i := 0; i < 256; i++ {
		b := uint8(0)

		meta := new(bucketmeta)
		for j := range meta.tophash8 {
			meta.tophash8[j] = b
		}

		finder := meta.Finder()
		empties := finder.EmptySlots()
		if empties.Count() == uint8(len(meta.tophash8)) {
			if b != 0 {
				t.Fatalf("got empties for %v", b)
			}
		} else {
			if b == 0 {
				t.Fatalf("did not get empties for %v", b)
			}
		}
	}
}

func TestPresent(t *testing.T) {
	for i := 0; i < 256; i++ {
		b := uint8(0)

		meta := new(bucketmeta)
		for j := range meta.tophash8 {
			meta.tophash8[j] = b
		}

		finder := meta.Finder()
		present := finder.PresentSlots()
		if present.Count() == uint8(len(meta.tophash8)) {
			if isMarkerTophash(b) {
				t.Fatalf("got present for %v", b)
			}
		} else {
			if !isMarkerTophash(b) {
				t.Fatalf("did not get present for %v", b)
			}
		}
	}
}
