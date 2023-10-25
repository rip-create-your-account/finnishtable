package finnishtable

import (
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"
)

func TestPhMap(t *testing.T) {
	var sizes = [...]int{
		0,
		1,
		2,
		4,
		7,
		13,
		29,
		63,
		77,
		121,
		146,
		189,
		204,
		263,
		1_023,
		1_902,
		6_021,
		10_518,
		39_127,
		76_124,
		124_152,
		1_012_912,
	}

	type tc struct {
		size int
	}
	testcases := make([]tc, 0, len(sizes))
	for _, size := range sizes {
		testcases = append(testcases, tc{size: size})
	}

	for tci := range testcases {
		size := testcases[tci].size

		t.Run(fmt.Sprintf("size=%v", size), func(t *testing.T) {
			hasher := memhash64

			kvs := make([]kv[int64, int64], size)
			for i := 0; i < size; i++ {
				kvs[i] = kv[int64, int64]{int64(i), int64(i)}
			}

			m := MakePerfectWithUnsafeHasher[int64, int64](hasher, kvs)
			if m.Len() != len(kvs) {
				t.Fatalf("want %v got %v", len(kvs), m.Len())
			}

			seenInts := make([]bool, size)
			for k := 0; k < size; k++ {
				v, ok := m.Get(int64(k))
				if !ok {
					t.Fatalf("missing %v=%v", k, v)
				}
				if v != int64(k) {
					t.Fatalf("wrong value for key %v, want %v got %v", k, k, v)
				}
				integer := m.GetInt(int64(k))
				if uint(integer) >= uint(size) {
					t.Fatalf("bad int for key %v, got %v", k, integer)
				}

				// check for GetInt duplicates
				if seenInts[integer] {
					t.Fatalf("duplicate %v", integer)
				}
				seenInts[integer] = true
			}

			// then some bad keys
			numBadKeys := size
			if numBadKeys < 1000 {
				numBadKeys = 1000
			}

			for k := size; k < size+numBadKeys; k++ {
				v, ok := m.Get(int64(k))
				if ok {
					t.Fatalf("matched key %v=%v", k, v)
				}
				integer := m.GetInt(int64(k))
				if integer != -1 {
					t.Fatalf("wrong int for key %v, want %v got %v", k, -1, integer)
				}
			}
		})
	}
}

func BenchmarkPhConstruct(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("size=%v", size), func(b *testing.B) {
			hasher := memhash64

			kvs := make([]kv[int64, int64], size)
			for i := 0; i < size; i++ {
				kvs[i] = kv[int64, int64]{int64(i), int64(i)}
			}

			b.ResetTimer()

			var m *PhFishTable[int64, int64]
			var min time.Duration = math.MaxInt64
			start := time.Now()
			lastSince := time.Since(start)
			for j := 0; j < b.N; j++ {
				m = MakePerfectWithUnsafeHasher[int64, int64](hasher, kvs)

				// retardo patronum!
				since := time.Since(start)
				d := since - lastSince
				if d < min {
					min = d
				}
				lastSince = since
			}
			b.StopTimer()

			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/elem")

			stats := phMapMemoryStats(m)
			bf := float64(stats.OccupiedBytes) / float64(stats.TotalSizeInBytes)
			esz := unsafe.Sizeof(m.allKvs[0])
			be := (float64(esz)/bf - float64(esz)) * 8
			b.ReportMetric(be, "bits/elem")
			b.ReportMetric(float64(min.Nanoseconds()), "ns/fastest")
		})
	}
}

func BenchmarkPhLookup(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("size=%v", size), func(b *testing.B) {
			hasher := memhash64

			kvs := make([]kv[int64, int64], size)
			for i := 0; i < size; i++ {
				kvs[i] = kv[int64, int64]{int64(i), int64(i)}
			}

			m := MakePerfectWithUnsafeHasher[int64, int64](hasher, kvs)

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					m.Get(int64(i))
				}
			}
			b.StopTimer()

			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/elem")
		})
	}
}

func phMapMemoryStats[K comparable, V any](m *PhFishTable[K, V]) MemoryStats {
	var totalMapsSizeInBytes int
	var occupiedBytes int

	totalMapsSizeInBytes += int(unsafe.Sizeof(m.trie[0])) * len(m.trie)
	totalMapsSizeInBytes += int(unsafe.Sizeof(m.allMetas[0])) * len(m.allMetas)
	totalMapsSizeInBytes += int(unsafe.Sizeof(m.allKvs[0])) * len(m.allKvs)
	occupiedBytes += int(unsafe.Sizeof(m.allKvs[0])) * len(m.allKvs)

	selfSize := int(unsafe.Sizeof(*m))

	return MemoryStats{
		OccupiedBytes:    uint64(occupiedBytes),
		TotalSizeInBytes: uint64(selfSize + totalMapsSizeInBytes),
	}
}
