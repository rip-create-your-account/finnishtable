package finnishtable

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"testing"
	"time"
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

	var presizes = [...]int{
		0, // no presizing
		1,
		15,
		61,
		372,
		21526,
	}

	type tc struct {
		size, presize int
		shittyHasher  bool
	}
	testcases := make([]tc, 0, len(sizes)*len(presizes))
	for _, size := range sizes {
		for _, presize := range presizes {
			testcases = append(testcases, tc{size: size, presize: presize})
		}
	}

	// Add some tests for shitty hash function
	for _, size := range []int{31, 69, 136, 211, 258, 310, 379, 420, 490, 513, 591, 790, 919, 1307, 1603, 2011, 2690, 7123} {
		testcases = append(testcases, tc{size: size, shittyHasher: true})
	}

	for tci := range testcases {
		size := testcases[tci].size
		presize := testcases[tci].presize
		shittyHasher := testcases[tci].shittyHasher

		t.Run(fmt.Sprintf("size=%v&presize=%v&shitty=%v", size, presize, shittyHasher), func(t *testing.T) {
			hasher := hasher
			if shittyHasher {
				shitseed := runtime_fastrand64()
				hasher = func(i int64, seed uint64) uint64 {
					return shitseed
				}
			}

			var m *FishTable[int64, int]
			if presize == 0 {
				m = MakeFishTable[int64, int](hasher)
			} else {
				m = MakeWithSize[int64, int](presize, hasher)
			}
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

					// Get should return the same value
					v2, ok := m.Get(k)
					if !ok {
						t.Fatalf("missing %v=%v", k, v)
					}
					if v2 != v {
						t.Fatalf("wrong value for key %v, want %v got %v", k, v, v2)
					}
					return true
				})
				if len(seen) != len(wantm) {
					t.Fatalf("got %v want %v", len(seen), len(wantm))
				}

				// Check map state
				var totalPop uint64
				m.iterateMaps(func(sm *smolMap2[int64, int]) bool {
					if sm.depth > 64 {
						t.Fatalf("bad depth %v", sm.depth)
					}
					if sm.bucketsPop() > maxEntriesPerMap || sm.bucketsPop() < 0 {
						t.Fatalf("bad pop %v", sm.bucketsPop())
					}
					if (sm.bucketsPop() + int(sm.tombs)) > maxEntriesPerMap {
						t.Fatalf("bad pop+tombs %v + %v = %v", sm.bucketsPop(), sm.tombs, sm.bucketsPop()+int(sm.tombs))
					}
					if sm.overflow != nil {
						if len(sm.overflow.hashes) == 0 {
							t.Fatalf("overflow arr should never be empty when overflow exists %v", len(sm.overflow.hashes))
						}
					}
					totalPop += uint64(sm.bucketsPop()) + uint64(sm.overflow.Len())
					return true
				})
				if totalPop != uint64(m.Len()) {
					t.Fatalf("bad total resident got %v m.Len()=%v", totalPop, m.Len())
				}
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
	3_298,
	6_021,
	10_518,
	39_127,
	76_124,
	124_152,
	2_500_000,
	9_648_634,
	100 * 1000 * 1000, // Mayor of the Cache-miss town
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
	return MakeWithUnsafeHasher[int64, int64](memhash64)
	return MakeFishTable[int64, int64](hasher)
}

var hasher = func(i int64, seed uint64) uint64 {
	h1 := memhash64(unsafe.Pointer(&i), uintptr(seed))
	return uint64(h1)
	// Shitty locality-sensitive hashing where certain bits are based on the
	// "location". Spread the bits to ensure that the trie doesn't end up
	// growing too fast for certain inputs.
	const lshbits = 0b1010_1010_1010_1010
	i >>= 16
	h2 := memhash64(unsafe.Pointer(&i), uintptr(seed))
	return (uint64(h1) &^ lshbits) ^ uint64(h2)
}

func BenchmarkInserts8B(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				m := makemap()
				for i := 0; i < size; i++ {
					m[int64(i)] = int64(i)
				}
				if len(m) != size {
					b.Fatal(len(m), size)
				}
			}
			b.StopTimer()
			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/insert")

			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()

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
			b.StopTimer()
			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/insert")
			b.ReportMetric(m.loadFactor(), "lf/map")

			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
		})
		b.Run(fmt.Sprintf("ext=sized&size=%v", size), func(b *testing.B) {
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()

			var m *FishTable[int64, int64]
			for j := 0; j < b.N; j++ {
				m = MakeWithSize[int64, int64](size, hasher)
				for i := 0; i < size; i++ {
					m.Put(int64(i), int64(i))
				}
				if m.Len() != size {
					b.Fatal(m.len, size)
				}
			}
			b.StopTimer()
			b.ReportMetric((float64(b.Elapsed()))/float64(b.N*size), "ns/insert")
			b.ReportMetric(m.loadFactor(), "lf/map")

			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
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

func BenchmarkDelete(b *testing.B) {
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			// El-oh-el
			ms := make([]map[int64]int64, b.N)
			for i := range ms {
				m := make(map[int64]int64, size)
				for i := 0; i < size; i++ {
					m[int64(i)] = int64(i)
				}
				ms[i] = m
			}
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				m := ms[j]
				for i := 0; i < size; i++ {
					delete(m, int64(i))
				}
				if len(m) != 0 {
					b.Fatal(len(m), 0)
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			// El-oh-el
			ms := make([]*FishTable[int64, int64], b.N)
			for i := range ms {
				m := MakeWithSize[int64, int64](size, hasher)
				for i := 0; i < size; i++ {
					m.Put(int64(i), int64(i))
				}
				ms[i] = m
			}
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				m := ms[j]
				for i := 0; i < size; i++ {
					m.Delete(int64(i))
				}
				if m.Len() != 0 {
					b.Fatal(m.Len(), 0)
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

func BenchmarkIterAfterMostDeleted(b *testing.B) {
	// NOTE: This benchmark is extremely FAIR because it showcases how stupid
	// it is to not shrink. FinnishTable shrinks, built-in go map doesn't.
	// Built-in map needs to iterate through hundreds of empty slots while
	// iterating trying to find those rare present slots.
	for si := range sizes {
		size := sizes[si]

		b.Run(fmt.Sprintf("ext=false&size=%v", size), func(b *testing.B) {
			m := makemap()
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}
			expectedSize := size
			for i := 1; i < size*99/100; i++ {
				delete(m, int64(i))
				expectedSize--
			}
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				var n int
				for range m {
					n++
				}
				if n != expectedSize {
					b.Fatal(n, expectedSize)
				}
			}
		})
		b.Run(fmt.Sprintf("ext=true&size=%v", size), func(b *testing.B) {
			m := makeextmap()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}
			expectedSize := size
			for i := 1; i < size*99/100; i++ {
				m.Delete(int64(i))
				expectedSize--
			}
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				var n int
				m.Iterate(func(k, v int64) bool {
					n++
					return true
				})
				if n != expectedSize {
					b.Fatal(n, expectedSize)
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
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()
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
			b.StopTimer()
			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
		})
		b.Run(fmt.Sprintf("ext=true&size=%v&times=%v", size, times), func(b *testing.B) {
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()
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
			b.StopTimer()
			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
		})
		b.Run(fmt.Sprintf("ext=sized&size=%v&times=%v", size, times), func(b *testing.B) {
			var memstats1 runtime.MemStats
			runtime.ReadMemStats(&memstats1)
			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for t := 0; t < times; t++ {
					m := MakeWithSize[int64, int64](size, hasher)
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
			b.StopTimer()
			var memstats2 runtime.MemStats
			runtime.ReadMemStats(&memstats2)
			b.ReportMetric(float64(memstats2.NumGC-memstats1.NumGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(memstats2.PauseTotalNs-memstats1.PauseTotalNs)/float64(b.N), "gc-stw-ns/op")
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

			runtime.GC() // for ns/insert accuracy

			start := time.Now()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}
			duration := time.Since(start)
			b.ReportMetric(float64(duration)/float64(size), "ns/insert")

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
		b.Run(fmt.Sprintf("ext=sized&size=%v", size), func(b *testing.B) {
			b.ReportMetric(0, "ns/op") // suppress the metric

			m := MakeWithSize[int64, int64](size, hasher)

			runtime.GC() // for ns/insert accuracy

			start := time.Now()
			for i := 0; i < size; i++ {
				m.Put(int64(i), int64(i))
			}
			duration := time.Since(start)
			b.ReportMetric(float64(duration)/float64(size), "ns/insert")

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
		occupiedBytes += sm.bucketsPop() * int(pairSize)

		if sm.overflow != nil {
			totalMapsSizeInBytes += int(unsafe.Sizeof(*sm.overflow))
			occupiedBytes += int(unsafe.Sizeof(*sm.overflow))
			if len(sm.overflow.hashes) > 0 {
				totalMapsSizeInBytes += int(unsafe.Sizeof(sm.overflow.hashes[0])) * cap(sm.overflow.hashes)
				totalMapsSizeInBytes += int(unsafe.Sizeof(sm.overflow.keys[0])) * cap(sm.overflow.keys)
				totalMapsSizeInBytes += int(unsafe.Sizeof(sm.overflow.values[0])) * cap(sm.overflow.values)

				occupiedBytes += int(unsafe.Sizeof(sm.overflow.hashes[0])) * len(sm.overflow.hashes)
				occupiedBytes += int(unsafe.Sizeof(sm.overflow.keys[0])) * len(sm.overflow.keys)
				occupiedBytes += int(unsafe.Sizeof(sm.overflow.values[0])) * len(sm.overflow.values)
			}
		}

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
