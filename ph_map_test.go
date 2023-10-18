package finnishtable

import (
	"fmt"
	"math"
	"testing"
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

			for k := 0; k < size; k++ {
				v, ok := m.Get(int64(k))
				if !ok {
					t.Fatalf("missing %v=%v", k, v)
				}
				if v != int64(k) {
					t.Fatalf("wrong value for key %v, want %v got %v", k, k, v)
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
			for j := 0; j < b.N; j++ {
				m = MakePerfectWithUnsafeHasher[int64, int64](hasher, kvs)
			}
			b.StopTimer()

			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/elem")
			b.ReportMetric((float64(b.Elapsed().Milliseconds()))/float64(b.N), "ms/map")

			stats := phMapMemoryStats(m)
			bf := float64(stats.OccupiedBytes) / float64(stats.TotalSizeInBytes)
			b.ReportMetric(bf, "xbytes-factor")
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
					v, ok := m.Get(int64(i))
					if v != int64(i) || !ok {
						b.Fatal(v, i, ok)
					}
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

func dumpPhDepths[K comparable, V any](m *phBuilder[K, V]) {
	depths := make(map[uint8]int, 64)
	m.iterateMapsWithRevisiting(func(i, f int, sm *phBuilderSmolMap[K, V]) {
		if i == f {
			depths[sm.depth]++
		}
	})
	var maxDepth uint8
	var minDepth uint8 = math.MaxUint8
	for depth := range depths {
		if depth > maxDepth {
			maxDepth = depth
		}
		if depth < minDepth {
			minDepth = depth
		}
	}

	println("depths:	level	maps")
	fmt.Printf("	[..]	0\n")
	for i := minDepth; i <= maxDepth; i++ {
		fmt.Printf("	[%02v]	%v\n", i, depths[i])
	}
	fmt.Printf("	[..]	0\n")
}
