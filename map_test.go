package finnishtable

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"unsafe"
)

//go:linkname memhash64 runtime.memhash64
//go:noescape
func memhash64(p unsafe.Pointer, h uintptr) uintptr

//go:linkname memhash runtime.memhash
//go:noescape
func memhash(p unsafe.Pointer, h, len uintptr) uintptr

//go:linkname strhash runtime.strhash
//go:noescape
func strhash(p unsafe.Pointer, h uintptr) uintptr

func TestMap(t *testing.T) {
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

	for si := range sizes {
		size := sizes[si]

		t.Run(fmt.Sprintf("size=%v", size), func(t *testing.T) {
			m := MakeFishTable[int64, int](hasher)
			wantm := make(map[int64]int, size)
			checkMaps := func() {
				if m.Len() != len(wantm) {
					t.Fatalf("got %v want %v", m.Len(), len(wantm))
				}

				// iterate should return the same values
				seen := make(map[int64]int, len(wantm))
				m.Iterate(func(k int64, v int) bool {
					if _, ok := wantm[k]; !ok {
						t.Fatalf("extra %v=%v", k, v)
					}
					if old, ok := seen[k]; ok {
						t.Fatalf("duplicate %v=%v, old=%v", k, v, old)
					}
					seen[k] = v
					return true
				})
				if len(seen) != len(wantm) {
					t.Fatalf("got %v want %v", len(seen), len(wantm))
				}

				// Get should return the same values
				for k1, v1 := range wantm {
					v2, ok := m.Get(k1)
					if !ok {
						t.Fatalf("missing %v=%v", k1, v1)
					}
					if v2 != v1 {
						t.Fatalf("wrong value for key %v, want %v got %v", k1, v1, v2)
					}
				}

				// Check map state
				m.iterateMaps(func(sm *smolMap2[int64, int]) bool {
					if sm.depth > 64 {
						t.Fatalf("bad depth %v", sm.depth)
					}
					if sm.pop > maxEntriesPerMap {
						t.Fatalf("bad pop %v", sm.pop)
					}
					if sm.tombs > maxEntriesPerMap {
						t.Fatalf("bad tombs %v", sm.tombs)
					}
					if (sm.pop + sm.tombs) > maxEntriesPerMap {
						t.Fatalf("bad pop+tombs %v + %v = %v", sm.pop, sm.tombs, sm.pop+sm.tombs)
					}
					return true
				})
			}

			for i := 0; i < size; i++ {
				m.Put(int64(i), i)
				wantm[int64(i)] = i
			}
			checkMaps()

			for i := 0; i < size; i += 2 {
				m.Delete(int64(i))
				delete(wantm, int64(i))
			}
			checkMaps()

			// Insert over tombstones
			for i := 0; i < size; i++ {
				m.Put(int64(i), i)
				wantm[int64(i)] = i
			}
			checkMaps()

			// Clear, map shrinking
			for i := 0; i < size; i++ {
				m.Delete(int64(i))
				delete(wantm, int64(i))
			}
			checkMaps()

			stats := mapMemoryStats(m)
			if stats.OccupiedBytes != 0 {
				t.Fatalf("map should be empty, had %v bytes worth of entries occupied", stats.OccupiedBytes)
			}
			// fmt.Printf("%v %vB %vB lf=%0.3f\n", size, stats.OccupiedBytes, stats.TotalSizeInBytes, float64(stats.OccupiedBytes)/float64(stats.TotalSizeInBytes))

			for i := size / 2; i < size+(size/2); i++ {
				m.Put(int64(i), i)
				wantm[int64(i)] = i
			}
			checkMaps()
		})
	}
}

var sizes = [...]int{
	1,
	7,
	13,
	29,
	63,
	121,
	263,
	723,
	1_023,
	1_902,
	6_021,
	10_518,
	39_127,
	76_124,
	124_152,
	9_648_634,
	// 100 * 1000 * 1000,
}

// Noinline so that built-in map doesn't get unfair optimizations that impact
// small size tests a lot.
//
//go:noinline
func makemap() map[int64]int64 {
	return make(map[int64]int64)
}

//go:noinline
func makeextmap() *FishTable[int64, int64] {
	return MakeFishTable[int64, int64](hasher)
}

var hasher = func(i int64, seed uint64) uint64 {
	h1 := memhash64(unsafe.Pointer(&i), uintptr(seed))
	return uint64(h1)
}

func BenchmarkInserts8B(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				m := makemap()
				for i := 0; i < size; i++ {
					m[int64(i)] = int64(i)
				}
				if len(m) != size {
					b.Fatal(len(m), size)
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			var m *FishTable[int64, int64]
			for j := 0; j < b.N; j++ {
				m = makeextmap()
				for i := 0; i < size; i++ {
					m.Put(int64(i), int64(i))
				}
				if m.Len() != size {
					b.Fatal(m.len, size)
				}
			}
		})
	}
}

func BenchmarkUpdate(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					m[int64(i)] = int64(i)
				}
			}
			if len(m) != size {
				b.Fatal(len(m), size)
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					m.Put(int64(i), int64(i))
				}
			}
		})
	}
}

func BenchmarkLookupHits(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					v, ok := m[int64(i)]
					if v != int64(i) || !ok {
						b.Fatal(v, i, ok)
					}
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					v, ok := m.Get(int64(i))
					if v != int64(i) || !ok {
						b.Fatal(v, i, ok)
					}
				}
			}
		})
	}
}

func BenchmarkLookupMisses(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					v, ok := m[int64(size+i)]
					if ok {
						b.Fatal(v, i)
					}
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for i := 0; i < size; i++ {
					v, ok := m.Get(int64(size + i))
					if ok {
						b.Fatal(v, i)
					}
				}
			}
		})
	}
}

func BenchmarkIter(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				var n int
				for range m {
					n++
				}
				if n != size {
					b.Fatal(n, size)
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				var n int
				m.Iterate(func(k, v int64) bool {
					n++
					return true
				})
				if n != size {
					b.Fatal(n, size)
				}
			}
		})
	}
}

func BenchmarkClone(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				m2 := makemap()
				var n int
				for k, v := range m {
					m2[k] = v
					n++
				}
				if n != size {
					b.Fatal("oops")
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				m2 := makeextmap()
				var n int
				m.Iterate(func(k, v int64) bool {
					m2.Put(k, v)
					n++
					return true
				})
				if n != size {
					b.Fatal("oops")
				}
			}
		})
	}
}

func BenchmarkShortUse(b *testing.B) {
	// This simulates the absolute classic usage of hashmaps - you create one,
	// populate it, do some lookups and then throw it away!

	sizes := [...]int{7, 33, 121, 795, 1092, 1976, 2359, 6215, 12412, 21962, 32469, 45950, 500_000}
	times := [...]int{10_000, 1_000, 1_000, 1_000, 1_000, 200, 200, 200, 200, 20, 20, 20, 5}
	for si := range sizes {
		size := sizes[si]
		times := times[si]

		b.Run(fmt.Sprintf("ext=false&size=%v&times=%v", size, times), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				for k := 0; k < times; k++ {
					m := makemap()
					for i := 0; i < size; i++ {
						m[int64(i)] = int64(i)
					}
					for i := 0; i < size; i++ {
						v, ok := m[int64(i)]
						if v != int64(i) || !ok {
							b.Fatal(v, i)
						}
					}
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v&times=%v", size, times), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				for t := 0; t < times; t++ {
					m := makeextmap()
					for i := 0; i < size; i++ {
						m.Put(int64(i), int64(i))
					}
					for i := 0; i < size; i++ {
						v, ok := m.Get(int64(i))
						if v != int64(i) || !ok {
							b.Fatal(v, i)
						}
					}
				}
			}
		})
	}
}

func BenchmarkLoadFactor(b *testing.B) {
	sizes := make([]int, 750)
	sizes[0] = 1
	sizes[1] = 2
	for i := 2; i < len(sizes); i++ {
		v := i + 1
		sizes[i] = v * v / 3
	}

	for si := range sizes {
		size := sizes[si]
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			b.ReportMetric(0, "ns/op") // suppress the metric

			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}

			// simple load factor
			occupied, totalSlots := m.load()
			if occupied != uint64(size) {
				b.Fatalf("got %v want %v", occupied, size)
			}
			lf := float64(occupied) / float64(totalSlots)
			b.ReportMetric(lf, "load-factor")

			// bytes
			// TODO: Convert slices to unsafe array pointers. So much wasted
			// bytes in storing the useless "capacity" for the slices
			stats := mapMemoryStats(m)
			bf := float64(stats.OccupiedBytes) / float64(stats.TotalSizeInBytes)
			b.ReportMetric(bf, "xbytes-factor")
		})
	}
}

func Benchmark21BKey(b *testing.B) {
	// NOTE: This test kind of cheats in the favor of the extendible hashing
	// implementation. The compiler is generating efficient code based on the
	// key and value types. Built-in map does not have that luxury.

	// NOTE: For small sizes compiler cheats in favor of the built-in map by
	// apparently stack-allocating the initial bucket for it.

	type key struct {
		n int64
		_ [21 - 8]byte
	}

	var hasher = func(i key, seed uint64) uint64 {
		h1 := memhash(unsafe.Pointer(&i), uintptr(seed), unsafe.Sizeof(i))
		return uint64(h1)
	}

	b.Run("inserts", func(b *testing.B) {
		for si := range sizes {
			size := sizes[si]

			b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					m := make(map[key]int64)
					for i := 0; i < size; i++ {
						m[key{n: int64(i)}] = int64(i)
					}
					if len(m) != size {
						b.Fatal(len(m), size)
					}
				}
			})
			b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
				var m *FishTable[key, int64]
				for j := 0; j < b.N; j++ {
					m = MakeFishTable[key, int64](hasher)
					for i := 0; i < size; i++ {
						m.Put(key{n: int64(i)}, int64(i))
					}
					if m.len != size {
						b.Fatal(m.len, size)
					}
				}
				b.StopTimer()

				// check that all is there
				seen := make(map[key]int64, size)
				m.Iterate(func(k key, v int64) bool {
					if v2, ok := seen[k]; ok {
						b.Fatal("duplicate", v, v2)
					}
					seen[k] = v
					if (key{n: v}) != k {
						b.Fatal("wrong value for the key")
					}
					return true
				})
				if len(seen) != size {
					b.Fatal("size mismatch", len(seen), size)
				}
			})
		}
	})

	b.Run("lookup-hits", func(b *testing.B) {
		for si := range sizes {
			size := sizes[si]

			b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
				m := make(map[key]int64)
				for i := 0; i < size; i++ {
					m[key{n: int64(i)}] = int64(i)
				}

				b.ResetTimer()
				for j := 0; j < b.N; j++ {
					for i := 0; i < size; i++ {
						v, ok := m[key{n: int64(i)}]
						if v != int64(i) || !ok {
							b.Fatal(v, i, ok)
						}
					}
				}
			})
			b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
				m := MakeFishTable[key, int64](hasher)
				for i := 0; i < size; i++ {
					m.Put(key{n: int64(i)}, int64(i))
				}

				b.ResetTimer()
				for j := 0; j < b.N; j++ {
					for i := 0; i < size; i++ {
						v, ok := m.Get(key{n: int64(i)})
						if v != int64(i) || !ok {
							b.Fatal(v, i, ok)
						}
					}
				}
			})
		}
	})
}

func BenchmarkStringKey(b *testing.B) {
	// NOTE: For small sizes compiler cheats in favor of the built-in map by
	// apparently stack-allocating the initial bucket for it.

	type key struct {
		n string
	}

	var hasher = func(i key, seed uint64) uint64 {
		h1 := strhash(unsafe.Pointer(&i.n), uintptr(seed))
		return uint64(h1)
	}

	b.Run("inserts", func(b *testing.B) {
		for si := range sizes {
			size := sizes[si]

			stringies := make([]string, size)
			for i := 0; i < size; i++ {
				stringies[i] = strconv.Itoa((i * 1_000_000) + 25_126_126)
			}

			b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					m := make(map[key]int64)
					for i := 0; i < size; i++ {
						m[key{n: stringies[i]}] = int64(i)
					}
					if len(m) != size {
						b.Fatal(len(m), size)
					}
				}
			})
			b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
				var m *FishTable[key, int64]
				for j := 0; j < b.N; j++ {
					m = MakeFishTable[key, int64](hasher)
					for i := 0; i < size; i++ {
						m.Put(key{n: stringies[i]}, int64(i))
					}
					if m.len != size {
						b.Fatal(m.len, size)
					}
				}
			})
		}
	})

	b.Run("lookup-hits", func(b *testing.B) {
		for si := range sizes {
			size := sizes[si]

			stringies := make([]string, size)
			for i := 0; i < size; i++ {
				stringies[i] = strconv.Itoa((i * 1_000_000) + 25_126_126)
			}

			b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
				m := make(map[key]int64)
				for i := 0; i < size; i++ {
					m[key{n: stringies[i]}] = int64(i)
				}

				b.ResetTimer()
				for j := 0; j < b.N; j++ {
					for i := 0; i < size; i++ {
						v, ok := m[key{n: stringies[i]}]
						if v != int64(i) || !ok {
							b.Fatal(v, i, ok)
						}
					}
				}
			})
			b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
				m := MakeFishTable[key, int64](hasher)
				for i := 0; i < size; i++ {
					m.Put(key{n: stringies[i]}, int64(i))
				}

				b.ResetTimer()
				for j := 0; j < b.N; j++ {
					for i := 0; i < size; i++ {
						v, ok := m.Get(key{n: stringies[i]})
						if v != int64(i) || !ok {
							b.Fatal(v, i, ok)
						}
					}
				}
			})
		}
	})
}

type MemoryStats struct {
	OccupiedBytes    uint64
	TotalSizeInBytes uint64
}

func mapMemoryStats[K comparable, V any](m *FishTable[K, V]) MemoryStats {
	var totalMapsSizeInBytes int
	var occupiedBytes int
	m.iterateMaps(func(sm *smolMap2[K, V]) bool {
		totalMapsSizeInBytes += (int(unsafe.Sizeof(*sm)))
		totalMapsSizeInBytes += (len(sm.bucketmetas)) * int(unsafe.Sizeof(sm.bucketmetas[0]))
		totalMapsSizeInBytes += (len(sm.buckets)) * int(unsafe.Sizeof(sm.buckets[0]))

		pairSize := unsafe.Sizeof(sm.buckets[0].keys[0]) + unsafe.Sizeof(sm.buckets[0].values[0])
		occupiedBytes += int(sm.pop) * int(pairSize)
		return true
	})

	selfSize := int(unsafe.Sizeof(*m))

	trieEntrySize := unsafe.Sizeof(m.trie[0])
	totalTrieSizeInBytes := int(trieEntrySize) * len(m.trie)
	return MemoryStats{
		OccupiedBytes:    uint64(occupiedBytes),
		TotalSizeInBytes: uint64(selfSize + totalTrieSizeInBytes + totalMapsSizeInBytes),
	}
}

func dumpDepths[K comparable, V any](m *FishTable[K, V]) {
	depths := make(map[uint8]int, 64)
	m.iterateMaps(func(sm *smolMap2[K, V]) bool {
		depths[sm.depth]++
		return true
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
