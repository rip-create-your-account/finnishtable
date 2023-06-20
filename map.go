package finnishtable

import (
	"math/bits"
	"unsafe"
	_ "unsafe"
)

//go:linkname runtime_fastrand64 runtime.fastrand64
func runtime_fastrand64() uint64

const (
	// Maximum number of buckets per map.
	// NOTE: Must be a power of two
	// NOTE: With our hashing scheme we can have up to 256 buckets per map.
	maxBuckets = 32

	// Aesthically pleasing load factor
	lfnumerator, lfdenominator = 27, 32

	maxEntriesPerMap = maxBuckets * bucketSize

	// How many splits to do before recalculating the hash for the keys? This
	// also ensures that at least 8-(triehashBoundary) bits are always usable
	// when matching hashes.
	triehashBoundary = 4

	// Maximum number of buckets to probe before switching to the sorted array.
	// This is also sets the worst case distance that Get and Delete need to
	// probe in the case of a lookup miss (if they don't see an empty slot
	// before this).
	//
	// Assume cache line of 64 bytes, 2 bytes of metadata per entry and 16
	// entries per bucket. We don't want to hit too many cache lines, me
	// thinks.
	maxProbeDistance = 8
)

type tophash = uint8

const (
	tophashEmpty     tophash = 0b0000_0000
	tophashTombstone tophash = 0b1000_0000
)

// fixTophash adjusts the hash so that it's never the marker for empty or
// tombstone
func fixTophash(hash uint8) tophash {
	// For current values of empty and tombstone the lower 7 bits are all zeros
	// and both "empty" and "tombstone" are the only two possible values with
	// that property.
	var add uint8
	if (hash & 0b0111_1111) == 0 {
		add = 1
	}
	return hash + add
}

func isMarkerTophash(hash tophash) bool {
	return hash&0b0111_1111 == 0
}

// holds metadata for each entry in the bucket
type bucketmeta struct {
	// top 8-bits of the hash adjusted with fixTophash()
	tophash8 [bucketSize]tophash
	// relevant 8-bits of the hash to avoid hashing keys all the time in split
	triehash8 [bucketSize]uint8
}

type bucket[K comparable, V any] struct {
	keys   [bucketSize]K
	values [bucketSize]V
}

func indexIntoBuckets(hash tophash, numBuckets int) uint8 {
	// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint8((uint16(hash) * uint16(numBuckets)) >> 8)
}

func nextIndexIntoBuckets(idx uint8, numBuckets int) uint8 {
	res := idx + 1
	m := uint8(0)
	if res < uint8(numBuckets) {
		m = 1
	}
	return res * m
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
// src/runtime/stubs.go
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x)
}

// A Finnish hash table, or fish for short. Stores key-value pairs.
type FishTable[K comparable, V any] struct {
	hasher func(key unsafe.Pointer, seed uintptr) uintptr
	trie   []*smolMap2[K, V] // TODO: Maybe store (smolMap.depth) here to get better use of the cache-lines
	seed   uintptr
	len    int

	// Real languages have bulk free() so freeing all of the the little maps
	// should be fast.
}

// Makes a Finnish table.
//
// Your mother will die in her sleep tonight unless you pass in a good hash
// function.
//
// NOTE: If you are okay with _just_ your aunt dying in her sleep tonight, you
// can try to smuggle some locality sensitive hashing bits into your hash
// function. Those bits should be put into the least significant bits of your
// hash value. Depending on how successful you are at it, you will end up
// finding that keys that are close to each other ended up into the same
// hashmap. For your access pattern this may mean much more effective use of
// your CPU caches.
func MakeFishTable[K comparable, V any](hasher func(key K, seed uint64) uint64) *FishTable[K, V] {
	m := new(FishTable[K, V])
	m.hasher = func(key unsafe.Pointer, seed uintptr) uintptr {
		return uintptr(hasher(*(*K)(key), uint64(seed)))
	}

	// The funny thing with random seed is that it makes cloning via
	// m.Iterate() slower by a good amount
	m.seed = uintptr(runtime_fastrand64())

	return m
}

func MakeWithSize[K comparable, V any](size int, hasher func(key K, seed uint64) uint64) *FishTable[K, V] {
	m := new(FishTable[K, V])
	m.hasher = func(key unsafe.Pointer, seed uintptr) uintptr {
		return uintptr(hasher(*(*K)(key), uint64(seed)))
	}

	// The funny thing with random seed is that it makes cloning via
	// m.Iterate() slower by a good amount
	m.seed = uintptr(runtime_fastrand64())

	if size <= 0 {
		return m
	}

	// Adjust size hint to accommodate for our max load factor.
	sizeAdjusted := size * 4 / 3
	if sizeAdjusted < size {
		sizeAdjusted = size
	}

	// Calculate how many maps and buckets per map we need.
	//
	// Here we prefer creating the least amount of maps necessary, creating as
	// big maps as possible.
	//
	// NOTE: The trie size MUST be a power of two but the number of buckets per
	// map doesn't need to be so.
	numMaps := sizeAdjusted / maxEntriesPerMap
	numMaps = 1 << bits.Len(uint(numMaps))
	numBuckets := sizeAdjusted / numMaps / bucketSize
	if numBuckets <= 0 {
		numBuckets = 1
	}
	if numBuckets > maxBuckets {
		panic("oops")
	}
	// The above math can give us a case like (numMaps==4 && numBuckets==16)
	// where we would instead prefer (numMaps==2 && numBuckets==32)
	if numMaps > 1 && numBuckets == maxBuckets/2 {
		numMaps >>= 1
		numBuckets *= 2
	}

	initialDepth := uint8(bits.TrailingZeros(uint(numMaps)))

	// TODO: WHERE'S MY BULK ALLOCATION?
	m.trie = make([]*smolMap2[K, V], numMaps)
	for i := range m.trie {
		m.trie[i] = makeSmolMap2[K, V](numBuckets)
		m.trie[i].depth = initialDepth
	}

	return m
}

func MakeWithUnsafeHasher[K comparable, V any](hasher func(key unsafe.Pointer, seed uintptr) uintptr) *FishTable[K, V] {
	m := new(FishTable[K, V])
	m.hasher = hasher

	// The funny thing with random seed is that it makes cloning via
	// m.Iterate() slower by a good amount
	m.seed = uintptr(runtime_fastrand64())

	return m
}

type smolMap2[K comparable, V any] struct {
	// NOTE: len(bucketmetas) is not always a power-of-two
	bucketmetas []bucketmeta
	// NOTE: len(buckets) is not always a power-of-two
	buckets []bucket[K, V] // TODO: Maybe instead just have ([]K, []V)

	// holds entries that didn't find an appropriate slot in the buckets
	overflow *sortedArr[K, V]

	depth      uint8 // <= 64
	preferGrow bool

	// total number of entries that can be added to the hash buckets before
	// reaching the load factor. Includes present entries and tombstones.
	// Excludes overflow len.
	unload int16 // ??? < x <= maxEntriesPerMap

	// TODO: Instead of keeping track of the number of tombstones we could just
	// always scan the bucketmetas when we need the information. We usually
	// need the count only when we are about to scan through bucketmetas
	// anyways.
	tombs uint16 // <= maxEntriesPerMap

	// NOTE: I'm pretty sure that a crazy person could pack all this into
	// 24 bytes or maybe even less. But not a gotard.
}

func (m *FishTable[K, V]) Len() int {
	return m.len
}

func (m *FishTable[K, V]) Get(k K) (V, bool) {
	trie := m.trie
	if len(trie) > 0 {
		hash := m.hasher(noescape(unsafe.Pointer(&k)), m.seed)

		sm := trie[hash&uintptr(len(trie)-1)]

		mapmetas := sm.bucketmetas // NOTE: Humbug! For large tries this is always a cache-miss
		if len(mapmetas) == 0 {    // nil if we shrunk very hard
			goto miss
		}

		tophash8 := fixTophash(uint8(hash >> 56))
		triehash8 := uint8(hash >> 0)

		if len(trie) >= (1 << triehashBoundary) {
			triehash8 = uint8(hash >> (sm.depth / triehashBoundary * triehashBoundary))
		}

		tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
		bucketIndex := indexIntoBuckets(tophash8, len(mapmetas)) // start looking from tophash8 location
		bucketsProbed := 1
		for {
			bucketmetas := &mapmetas[bucketIndex]
			finder := bucketmetas.Finder()

			hashMatches, empties := finder.ProbeHashMatchesAndEmpties(tophashProbe, triehashProbe)
			for ; hashMatches.HasCurrent(); hashMatches.Advance() {
				idx := hashMatches.Current()

				bucket := &sm.buckets[bucketIndex]
				if bucket.keys[idx] == k {
					return bucket.values[idx], true
				}
			}

			if empties.HasCurrent() {
				// No need to look further - the probing during inserting would
				// have placed the key into one of these empty slots.
				goto miss
			}
			if bucketsProbed == len(mapmetas) {
				goto miss
			}
			if bucketsProbed >= maxProbeDistance {
				// Check overflow
				if sm.overflow != nil {
					v, ok := sm.overflow.Get(k, tophash8, triehash8)
					if ok {
						return v, true
					}
				}
				goto miss
			}

			bucketIndex = nextIndexIntoBuckets(bucketIndex, len(mapmetas))
			bucketsProbed++
		}
	}

miss:
	var zerov V
	return zerov, false
}

func (m *FishTable[K, V]) Delete(k K) {
	trie := m.trie
	if len(trie) == 0 {
		return
	}

	hash := m.hasher(noescape(unsafe.Pointer(&k)), m.seed)

	sm := trie[hash&uintptr(len(trie)-1)]

	mapmetas := sm.bucketmetas // NOTE: Humbug! For large tries this is always a cache-miss
	if len(mapmetas) == 0 {    // nil if we shrunk very hard
		return
	}

	tophash8 := fixTophash(uint8(hash >> 56))
	triehash8 := uint8(hash >> 0)

	if len(trie) >= (1 << triehashBoundary) {
		triehash8 = uint8(hash >> (sm.depth / triehashBoundary * triehashBoundary))
	}

	tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
	bucketIndex := indexIntoBuckets(tophash8, len(mapmetas)) // start looking from tophash8 location

	bucketsProbed := 1
	for {
		bucketmetas := &mapmetas[bucketIndex]
		finder := bucketmetas.Finder()

		hashMatches, empties := finder.ProbeHashMatchesAndEmpties(tophashProbe, triehashProbe)
		for ; hashMatches.HasCurrent(); hashMatches.Advance() {
			slotInBucket := hashMatches.Current()

			bucket := &sm.buckets[bucketIndex]
			if bucket.keys[slotInBucket] == k {
				// Found it!

				var zerok K
				var zerov V
				bucket.keys[slotInBucket] = zerok
				bucket.values[slotInBucket] = zerov

				// If this bucket has empty slots then we know that no-one has
				// inserted past this bucket (any interested party would have
				// inserted into one of the empty slots). Which means we don't
				// need to leave a tombstone to force probes to go onwards to
				// the following buckets.
				if empties.HasCurrent() {
					bucketmetas.tophash8[slotInBucket] = tophashEmpty
					sm.unload++
				} else {
					bucketmetas.tophash8[slotInBucket] = tophashTombstone
					sm.tombs++
				}

				bucketmetas.triehash8[slotInBucket] = 0
				m.len--

				// Try to have an overflow entry take the slot. This ensures
				// that the overflow doesn't contain any entries that could
				// exist in the buckets, meaning that Put/Get don't need to
				// even look at the overflow unless the probing "overflows."
				//
				// NOTE: Such entry for this slot could be in the overflow only
				// if this bucket had no empties.
				if sm.overflow != nil {
					if !empties.HasCurrent() && sm.overflow.TryToMoveBackToBucket(bucketmetas, bucket, bucketIndex, slotInBucket, len(mapmetas)) {
						sm.tombs--
						sm.unload += 0 // -1 +1
						if len(sm.overflow.hashes) == 0 {
							sm.overflow = nil
						}
					}
				}

				// Shrink if ~3/4 slots free.
				if int(sm.unload) >= len(mapmetas)*bucketSize*3/4 {
					// When empty, release the buckets
					if int(sm.unload) == len(mapmetas)*bucketSize {
						sm.bucketmetas = nil
						sm.buckets = nil
						sm.unload = 0
						sm.tombs = 0
					} else if didShrink := m.maybeShrinkByFusing(sm, hash); didShrink {
					} else if len(sm.bucketmetas) > 1 {
						// TODO: Do we ever want to shrink by allocating a
						// smaller array of buckets? Maybe when 1/8 slots used
						// or something like that.
						// m.inplaceShrinkSmol(sm)
						// sm.preferGrow = true
					}
				}
				return
			}
		}

		if empties.HasCurrent() {
			// No need to look further - the probing during inserting would
			// have placed the key into one of these empty slots.
			return
		}
		if bucketsProbed == len(mapmetas) {
			return
		}
		if bucketsProbed >= maxProbeDistance {
			// Check overflow
			if sm.overflow != nil {
				if sm.overflow.Delete(k, tophash8, triehash8) {
					m.len--
					if len(sm.overflow.hashes) == 0 {
						sm.overflow = nil
					}
				}
			}
			return
		}

		bucketIndex = nextIndexIntoBuckets(bucketIndex, len(mapmetas))
		bucketsProbed++
	}
}

// Does the Put thing.
func (m *FishTable[K, V]) Put(k K, v V) {
	hash := m.hasher(noescape(unsafe.Pointer(&k)), m.seed)
	trie := m.trie
	if len(trie) == 0 {
		// fused alloc for first Put
		type first[K comparable, V any] struct {
			bucketmetas [1]bucketmeta
			trie        [1]*smolMap2[K, V]
			buckets     [1]bucket[K, V]
			sm          smolMap2[K, V]
		}
		arrs := new(first[K, V])
		sm := &arrs.sm

		sm.bucketmetas = arrs.bucketmetas[:]
		sm.buckets = arrs.buckets[:]

		arrs.trie[0] = sm
		m.trie = arrs.trie[:1:len(arrs.trie)]

		tophash8 := fixTophash(uint8(hash >> 56))
		triehash8 := uint8(hash >> 0)
		arrs.bucketmetas[0].tophash8[0] = tophash8
		arrs.bucketmetas[0].triehash8[0] = triehash8
		arrs.buckets[0].keys[0] = k
		arrs.buckets[0].values[0] = v
		sm.unload = sm.maxLoad()
		sm.unload--
		m.len++
		return
	}

top:
	sm := trie[hash&uintptr(len(trie)-1)]

	tophash8 := fixTophash(uint8(hash >> 56))
	triehash8 := uint8(hash >> 0)

	mapmetas := sm.bucketmetas // NOTE: Humbug! For large tries this is always a cache-miss
	if len(mapmetas) == 0 {    // nil if we shrunk very hard
		sm.bucketmetas = make([]bucketmeta, 1)
		sm.buckets = make([]bucket[K, V], 1)
		sm.unload = sm.maxLoad()
		mapmetas = sm.bucketmetas
	}

	if len(trie) >= (1 << triehashBoundary) {
		triehash8 = uint8(hash >> (sm.depth / triehashBoundary * triehashBoundary))
	}

	tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
	bucketIndex := indexIntoBuckets(tophash8, len(mapmetas)) // start looking from tophash8 location

	bucketsProbed := 1
	for {
		bucketmetas := &mapmetas[bucketIndex]
		finder := bucketmetas.Finder()

		hashMatches, empties := finder.ProbeHashMatchesAndEmpties(tophashProbe, triehashProbe)
		for ; hashMatches.HasCurrent(); hashMatches.Advance() {
			idx := hashMatches.Current()

			bucket := &sm.buckets[bucketIndex]
			if bucket.keys[idx] == k {
				bucket.values[idx] = v
				return
			}
		}

		// TODO: A fun and interesting thing would be to give priority access
		// to the optimal buckets to those keys that would in the next grow
		// belong to the "left" child. This would mean that when split()ing,
		// those entries need not be moved at all. We would only need to read
		// their tophash and triehash. No touching of its keys or values.
		// Experiment having them take places from the "right" entries.

		// If we see an empty slot then we can take it. Some earlier insert
		// with the same key would have already been found as part of the
		// probing, so we know that the key doesn't exist in the map yet.
		if empties.HasCurrent() {
			idx := empties.Current()

			// TODO: If we saw a tombstone along the way then we could also
			// take that spot. But do I care to? NO!

			bucket := &sm.buckets[bucketIndex]
			bucket.keys[idx] = k
			bucket.values[idx] = v
			bucketmetas.tophash8[idx] = tophash8
			bucketmetas.triehash8[idx] = triehash8
			sm.unload--
			m.len++

			// Check if we should grow. It's good to do it now that we quite
			// likely probed through a good chunk of the map. Surely data is
			// now in the CPU caches.
			if sm.unload > 0 {
				return
			}

			m.makeSpaceForMap(sm, hash)
			return
		}

		if bucketsProbed >= len(mapmetas) {
			// THIS IS WHERE WE GROW THEM!
			m.makeSpaceForMap(sm, hash)
			trie = m.trie
			goto top
		}

		if bucketsProbed >= maxProbeDistance {
			sm.ensureOverflow()
			if updated := sm.overflow.Put(k, v, tophash8, triehash8); updated {
				return
			}

			m.len++

			// NOTE: Overflow doesn't count towards load because the hash
			// function could shit the bed and end up putting hundreds of
			// entries into the same overflow. We don't want to be trying to
			// grow our map/trie in that case.

			// But we should still check the load. Sometimes after splitting
			// ALL ENTRIES go to one map so this code path is the only thing
			// that would ever get taken.
			if sm.unload <= 0 {
				m.makeSpaceForMap(sm, hash)
			}
			return
		}

		bucketIndex = nextIndexIntoBuckets(bucketIndex, len(mapmetas))
		bucketsProbed++
	}
}

// A fine optimization. No good reason to have it disabled. This variable
// exists to mark all the places where this allocation can cause headaches.
const sharedAllocOptimizationEnabled = true

func (m *FishTable[K, V]) makeSpaceForMap(sm *smolMap2[K, V], hash uintptr) {
	// Just rehash if map has tombstones
	// TODO: Adjust condition
	if sm.tombs > 0 {
		m.rehashInplace(sm)
		return
	}

	// Optimization for growth. We grow the sibling map as well if it's full
	// enough. This allows us to do it in one big allocation which we can then
	// later on turn into a single larger map. Initially the allocation is
	// split equally between the two maps but the map that then becomes full
	// first will kick off the other map and take the memory area all for
	// himself, rehashing into it.
	//
	// We do waste tiny amount of load factor here but we save a lot memory if
	// we end up growing once more after this growth, so it more than balances
	// out.
	if sharedAllocOptimizationEnabled && sm.depth > 0 {
		hibi := uintptr(1) << uintptr(sm.depth-1)
		step := hibi
		siblingPos := hash & (hibi - 1)
		firstMap := m.trie[siblingPos]
		secondMap := m.trie[siblingPos+step]

		self := firstMap
		other := secondMap
		if sm != self {
			self, other = other, self
		}

		// Because of various growth/shrink sequences we could end up into a
		// situation where the two maps don't share the allocation anymore.
		//
		// NOTE: If the other map has been doing some hardcore Deletes then it
		// has no buckets.
		var allocIsShared bool
		if cap(firstMap.bucketmetas) > len(firstMap.bucketmetas) && len(secondMap.bucketmetas) > 0 {
			extended := firstMap.bucketmetas[:cap(firstMap.bucketmetas):cap(firstMap.bucketmetas)]

			// NOTE: POINTER EQUALITY. DO THE TWO POINTERS POINT TO THE SAME MEMORY?
			if &extended[len(firstMap.bucketmetas)] == &secondMap.bucketmetas[0] {
				allocIsShared = true
			}
		}

		// Maybe some weird case after crazy sequence of growths and shrinks
		if !allocIsShared && len(self.bucketmetas) < cap(self.bucketmetas) {
			// Expand and then rehash it to use the newly
			// available space.

			oldBucketsOffset := 0
			oldBucketsLen := len(self.bucketmetas)
			load := self.bucketsPop()

			self.bucketmetas = self.bucketmetas[:cap(self.bucketmetas):cap(self.bucketmetas)]
			self.buckets = self.buckets[:cap(self.buckets):cap(self.buckets)]
			self.unload = self.maxLoad() - int16(load)

			m.rehashInplaceAfterResizing(self, oldBucketsOffset, oldBucketsLen)
			self.preferGrow = false // split next time
			return
		}

		// Not really our sibling if not on the same depth
		if self.depth != other.depth {
			goto normalGrowth
		}

		isBigEnoughToGrowEarly := func(sm *smolMap2[K, V]) bool {
			return sm.unload <= (sm.maxLoad() / 4)
		}

		doSharedAlloc := func() {
			temp := makeSmolMap2[K, V](len(self.bucketmetas) * 2 * 2)
			fullMetas := temp.bucketmetas
			fullBuckets := temp.buckets

			temp.depth = firstMap.depth
			temp.bucketmetas = fullMetas[:len(fullMetas)/2]
			temp.buckets = fullBuckets[:len(fullBuckets)/2]
			temp.unload = temp.maxLoad()
			m.moveSmol(temp, firstMap)
			*firstMap = *temp
			firstMap.preferGrow = false // split next time

			*temp = smolMap2[K, V]{}
			temp.depth = secondMap.depth
			temp.bucketmetas = fullMetas[len(fullMetas)/2:]
			temp.buckets = fullBuckets[len(fullBuckets)/2:]
			temp.unload = temp.maxLoad()
			m.moveSmol(temp, secondMap)
			*secondMap = *temp
			secondMap.preferGrow = false // split next time
		}

		// Did the previous growth set up this optimization so that we could
		// now collect our harvest?
		if allocIsShared {
			bigBucketmetas := firstMap.bucketmetas
			bigBuckets := firstMap.buckets

			oldOtherBuckets := other.buckets
			oldOtherMetas := other.bucketmetas
			selfPop := self.bucketsPop()

			// Move the smaller of the two to its own allocation, also use this
			// opportunity to grow it if it's full enough.
			//
			// But if both maps are very full... Let's just do shared alloc for
			// them again!
			otherIsFullEnough := isBigEnoughToGrowEarly(other)
			if otherIsFullEnough {
				if len(self.bucketmetas)*2*2 <= maxBuckets {
					doSharedAlloc()
					return
				}
				m.inplaceGrowSmol(other)
				other.preferGrow = false // split next time
			} else {
				m.duplicateInplace(other)
			}

			// We moved the entries from other to a new allocation. Now, before
			// we can use it for self we really ought to zero the memory areas
			// previously used by other.
			for i := range oldOtherMetas {
				oldOtherMetas[i] = bucketmeta{}
			}
			for i := range oldOtherBuckets {
				oldOtherBuckets[i] = bucket[K, V]{}
			}

			// Let the lucky one expand and then rehash it to use the newly available space
			oldBucketsOffset := 0
			if self == secondMap {
				oldBucketsOffset = len(self.bucketmetas)
			}
			oldBucketsLen := len(self.bucketmetas)

			self.bucketmetas = bigBucketmetas[:cap(bigBucketmetas):cap(bigBucketmetas)]
			self.buckets = bigBuckets[:cap(bigBuckets):cap(bigBuckets)]
			self.unload = self.maxLoad() - int16(selfPop)

			m.rehashInplaceAfterResizing(self, oldBucketsOffset, oldBucketsLen)
			self.preferGrow = false // split next time
			return
		}

		// If the maps can have different number of buckets then the algorithm
		// would need to be slightly more complex. Don't even try.
		if len(self.bucketmetas) != len(other.bucketmetas) {
			goto normalGrowth
		}

		// Are we too big to do this optimization?
		if len(self.bucketmetas)*2*2 > maxBuckets {
			goto normalGrowth
		}

		// Is the other map full enough to grow with us?
		otherIsFullEnough := isBigEnoughToGrowEarly(other)
		if otherIsFullEnough {
			doSharedAlloc()
			return
		}
	}
	if sharedAllocOptimizationEnabled && sm.depth == 0 {
		// If we shrunk after doing this optimization and now much later we are
		// growing again we will have place to grow into.
		if len(sm.bucketmetas)*2 == cap(sm.bucketmetas) {
			oldBucketsOffset := 0
			oldBucketsLen := len(sm.bucketmetas)
			selfPop := sm.bucketsPop()

			sm.bucketmetas = sm.bucketmetas[:cap(sm.bucketmetas):cap(sm.bucketmetas)]
			sm.buckets = sm.buckets[:cap(sm.buckets):cap(sm.buckets)]
			sm.unload = sm.maxLoad() - int16(selfPop)

			m.rehashInplaceAfterResizing(sm, oldBucketsOffset, oldBucketsLen)
			sm.preferGrow = false // split next time
			return
		}
	}
normalGrowth:

	// Instead of always splitting, we just add some more buckets to the map.
	// Also it's nice to try to milk as much as possible out of the initial
	// triehash8.
	reallyShouldPreferGrow := sm.depth >= (triehashBoundary - 1)
	if (reallyShouldPreferGrow || sm.preferGrow) && len(sm.bucketmetas) < maxBuckets {
		m.inplaceGrowSmol(sm)
		sm.preferGrow = false // split next time
		return
	}

	// split it
	left, right := m.split(sm)

	if left != sm {
		// the trie update loop below requires this
		panic("oops")
	}

	// add more buckets next time
	left.preferGrow = true
	right.preferGrow = true

	// But maybe the trie needs to grow as well...
	oldDepth := left.depth - 1
	if 1<<oldDepth == len(m.trie) {
		oldTrie := m.trie
		m.trie = make([]*smolMap2[K, V], len(oldTrie)*2)
		copy(m.trie[:len(oldTrie)], oldTrie)
		copy(m.trie[len(oldTrie):], oldTrie)
	}

	hibi := uintptr(1) << uintptr(oldDepth)
	step := hibi
	for i := uintptr(hash&(hibi-1)) + step; i < uintptr(len(m.trie)); i += (step * 2) {
		m.trie[i] = right
	}
}

type bucketInserter [maxBuckets]uint8

func (freeSlots *bucketInserter) findSlot(preferredBucketIdx uint8, numBuckets int) (uint8, uint8) {
	bucketIdx := preferredBucketIdx
	var bucketsProbed int
	for {
		nextFreeInBucket := freeSlots[bucketIdx]
		if nextFreeInBucket >= bucketSize {
			bucketIdx = nextIndexIntoBuckets(bucketIdx, numBuckets)
			bucketsProbed++
			if bucketsProbed >= numBuckets || bucketsProbed >= maxProbeDistance {
				return uint8(numBuckets), nextFreeInBucket
			}
			continue
		}
		freeSlots[bucketIdx]++
		return bucketIdx, nextFreeInBucket
	}
}

type bucketTracker [maxBuckets]bucketBitmask

func (tracker *bucketTracker) findSlot(preferredBucketIdx uint8, numBuckets int, unfortunateBucket, unfortunateSlot uint8) (uint8, uint8) {
	_ = tracker[0] // nil check out of the loop

	bucketIdx := preferredBucketIdx
	var bucketsProbed int
	for {
		nextFreeInBucket := tracker[bucketIdx].FirstUnmarkedSlot()
		if nextFreeInBucket >= bucketSize {
			bucketIdx = nextIndexIntoBuckets(bucketIdx, numBuckets)
			bucketsProbed++
			if bucketsProbed >= numBuckets || bucketsProbed >= maxProbeDistance {
				return uint8(numBuckets), nextFreeInBucket
			}
			continue
		}

		// Try to prefer the given slot if we landed all the way into the
		// unfortunate bucket
		if bucketIdx == unfortunateBucket && !tracker[bucketIdx].IsMarked(unfortunateSlot) {
			nextFreeInBucket = unfortunateSlot
		}

		tracker[bucketIdx].Mark(nextFreeInBucket)
		return bucketIdx, nextFreeInBucket
	}
}

type slotPtr[K comparable, V any] struct {
	key       *K
	value     *V
	tophash8  *tophash
	triehash8 *uint8
}

func makeSlotPtr[K comparable, V any](metas *bucketmeta, bucket *bucket[K, V], slotInBucket uint8) slotPtr[K, V] {
	return slotPtr[K, V]{
		key:       &bucket.keys[slotInBucket],
		value:     &bucket.values[slotInBucket],
		tophash8:  &metas.tophash8[slotInBucket],
		triehash8: &metas.triehash8[slotInBucket],
	}
}

func makeOverflowSlotPtr[K comparable, V any](ov *sortedArr[K, V], i int) slotPtr[K, V] {
	return slotPtr[K, V]{
		key:       &ov.keys[i],
		value:     &ov.values[i],
		tophash8:  &ov.hashes[i].tophash8,
		triehash8: &ov.hashes[i].triehash8,
	}
}

func writeSlot[K comparable, V any](dst, src slotPtr[K, V]) {
	*dst.key = *src.key
	*dst.value = *src.value
	*dst.tophash8 = *src.tophash8
	*dst.triehash8 = *src.triehash8
}

func swapSlots[K comparable, V any](a, b slotPtr[K, V]) {
	*a.key, *b.key = *b.key, *a.key
	*a.value, *b.value = *b.value, *a.value
	*a.tophash8, *b.tophash8 = *b.tophash8, *a.tophash8
	*a.triehash8, *b.triehash8 = *b.triehash8, *a.triehash8
}

func zeroSlot[K comparable, V any](a slotPtr[K, V]) {
	var zerok K
	var zerov V
	*a.key = zerok
	*a.value = zerov
	*a.tophash8 = tophashEmpty
	*a.triehash8 = 0
}

// splits m into two maps (where the "left" map is the 'm' re-used). Cleans up
// all the tombstones from 'm'.
func (h *FishTable[K, V]) split(m *smolMap2[K, V]) (*smolMap2[K, V], *smolMap2[K, V]) {
	if m.depth == 64 {
		// 64-bit hash values have only 64 bits :-(
		panic("depth overflow")
	}

	m.depth++

	// Re-use m as left
	left := m

	right := makeSmolMap2[K, V](len(left.bucketmetas))
	right.depth = m.depth

	oldDepthBit := uint8(1 << ((m.depth - 1) % triehashBoundary))
	crossedTriehashBoundary := (m.depth % triehashBoundary) == 0

	// Prove some stuff to the compiler so that the loops don't generate so
	// many bounds checks.
	if len(m.bucketmetas) != len(right.bucketmetas) {
		panic("oops")
	}
	if len(m.buckets) != len(right.buckets) {
		panic("oops")
	}
	if len(m.bucketmetas) != len(m.buckets) {
		panic("oops")
	}
	if len(right.bucketmetas) != len(right.buckets) {
		panic("oops")
	}
	if len(m.bucketmetas) > maxBuckets {
		panic("oops")
	}

	rehash := func(key *K, outTriehash8 *uint8) {
		hash := h.hasher(unsafe.Pointer(key), h.seed)
		*outTriehash8 = uint8(hash >> (m.depth / triehashBoundary * triehashBoundary))
	}

	oldOverflow := m.overflow
	m.overflow = nil

	var previousBucketHadEmpties bool
	if len(m.bucketmetas) > 1 {
		bucketmetas := &m.bucketmetas[len(m.bucketmetas)-1]
		finder := bucketmetas.Finder()
		empties := finder.EmptySlots()
		previousBucketHadEmpties = empties.HasCurrent()
	}

	var rightInserter bucketInserter

	moveToRight := func(slot slotPtr[K, V]) {
		if crossedTriehashBoundary {
			rehash(slot.key, slot.triehash8)
		}

		preferredBucketIdx := indexIntoBuckets(*slot.tophash8, len(right.bucketmetas))
		bucketIdx, nextFreeInBucket := rightInserter.findSlot(preferredBucketIdx, len(right.bucketmetas))
		if nextFreeInBucket < bucketSize {
			dstmetas := &right.bucketmetas[bucketIdx]
			dstbuckets := &right.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
			writeSlot(dstSlot, slot)
			right.unload--
		} else {
			right.ensureOverflow()
			right.overflow.PutNotUpdateSlotPtr(slot)
		}

		// Zero the slot left behind. Only setting the hashes is strictly necessary
		zeroSlot(slot)
	}

	var leftTracker bucketTracker
	for bucketIndex := range m.bucketmetas {
		bucketmetas := &m.bucketmetas[bucketIndex]
		bucket := &m.buckets[bucketIndex]
		trackingBucket := &leftTracker[bucketIndex]

		finder := bucketmetas.Finder()

		empties := finder.EmptySlots()
		hadEmpties := empties.HasCurrent()

		// bother to clear tombies if we are planning on reusing m
		if left.tombs > 0 {
			finder.MakeTombstonesIntoEmpties(bucketmetas)
		}

		presentSlots := finder.PresentSlots()

		// Figure out righties. Remember to exclude the already placed entries
		// because already placed entries have a new triehash if
		// crossedTriehashBoundary==true
		righties := finder.GoesRightForBit(oldDepthBit)
		righties = righties.WithBitmaskExcluded(*trackingBucket)

		newLefties := presentSlots.WithBitmaskExcluded(righties.AsBitmask() | (*trackingBucket))

		if crossedTriehashBoundary {
			entries := (righties.AsBitmask() | newLefties.AsBitmask()).AsMatchiter()
			for ; entries.HasCurrent(); entries.Advance() {
				slotInBucket := entries.Current()
				rehash(&bucket.keys[slotInBucket], &bucketmetas.triehash8[slotInBucket])
			}
		}

		// If the previous bucket had empty slots before splitting, then we
		// know that all of the entries in this bucket are in their optimal
		// bucket AND that is the case quite often.
		if previousBucketHadEmpties {
			*trackingBucket |= newLefties.AsBitmask()
			newLefties = matchiter{}
		}

		// Move righties to the right map
		left.unload += int16(righties.Count())
		for ; righties.HasCurrent(); righties.Advance() {
			slotInBucket := righties.Current()
			srcSlot := makeSlotPtr(bucketmetas, bucket, slotInBucket)

			// NOTE: This block of code is inlined "moveToRight." For speed
			// because ~50% of entries are moved to the "right" map.

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(right.bucketmetas))
			bucketIdx, nextFreeInBucket := rightInserter.findSlot(preferredBucketIdx, len(right.bucketmetas))
			if nextFreeInBucket < bucketSize {
				dstmetas := &right.bucketmetas[bucketIdx]
				dstbuckets := &right.buckets[bucketIdx]
				dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
				writeSlot(dstSlot, srcSlot)
				right.unload--
			} else {
				right.ensureOverflow()
				right.overflow.PutNotUpdateSlotPtr(srcSlot)
			}

			// Zero the slot left behind. Only setting the hashes is strictly necessary
			zeroSlot(srcSlot)
		}

		// Move entries in the left closer to their optimal buckets
		//
		// TODO: Use SIMD to filter out all of the lefties that don't need
		// moving. Only ~1/20 of lefties need moving. But here we are entering
		// the loop for almost 1/2 of the lefties. It would also allow us to
		// get rid of the lesser "previousBucketHadEmpties" optimization.
		for ; newLefties.HasCurrent(); newLefties.Advance() {
			slotInBucket := newLefties.Current()
			srcSlot := makeSlotPtr(bucketmetas, bucket, slotInBucket)

			for {
				preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(left.bucketmetas))

				// If we will stay in the same bucket then we can just stay in the slot
				// that we are currently sitting in
				if uint8(bucketIndex) == preferredBucketIdx {
					// ensure that the slot is marked as taken
					trackingBucket.Mark(slotInBucket)
					break
				}

			findSlot:
				foundBucketIdx, nextFreeInBucket := leftTracker.findSlot(preferredBucketIdx, len(left.bucketmetas), uint8(bucketIndex), slotInBucket)
				if nextFreeInBucket >= bucketSize {
					// to the overflow!
					left.ensureOverflow()
					left.overflow.PutNotUpdateSlotPtr(srcSlot)
					left.unload++

					// zero the slot left behind
					zeroSlot(srcSlot)
					break
				}

				// When already tightly packed, we place into the exact same bucket
				if foundBucketIdx == uint8(bucketIndex) && nextFreeInBucket == slotInBucket {
					break
				}

				dstMetas := &m.bucketmetas[foundBucketIdx]
				dstBuckets := &m.buckets[foundBucketIdx]

				dstSlot := makeSlotPtr(dstMetas, dstBuckets, nextFreeInBucket)

				// There could be a present entry at that slot that deserves to be there
				if !isMarkerTophash(*dstSlot.tophash8) {
					goesRight := (*dstSlot.triehash8)&oldDepthBit > 0
					if goesRight {
						moveToRight(dstSlot)
						left.unload++
					} else {
						victimPreferredBucket := indexIntoBuckets(*dstSlot.tophash8, len(left.bucketmetas))
						if foundBucketIdx == victimPreferredBucket {
							// Victim gets to keep its slot.
							if crossedTriehashBoundary {
								rehash(dstSlot.key, dstSlot.triehash8)
							}
							// We will find some other slut for ourselves
							goto findSlot
						}
					}
				}

				// Are we about to swap places with a tombstone?
				if *dstSlot.tophash8 == tophashTombstone {
					*dstSlot.tophash8 = tophashEmpty
				}

				// swap, nicely zeroes the slot left behind if we are inserting into an
				// empty slot
				swapSlots(dstSlot, srcSlot)

				// Did we swap in an empty slot or a present one?
				if *srcSlot.tophash8 == tophashEmpty {
					break
				}

				if crossedTriehashBoundary {
					rehash(srcSlot.key, srcSlot.triehash8)
				}
			}
		}

		previousBucketHadEmpties = hadEmpties
	}

	// Then the overflows :-)
	if ov := oldOverflow; ov != nil {
		for i := range ov.hashes {
			srcSlot := makeOverflowSlotPtr(ov, i)

			goesRight := (*srcSlot.triehash8)&oldDepthBit > 0

			if crossedTriehashBoundary {
				rehash(&ov.keys[i], srcSlot.triehash8)
			}

			dst := left
			var bucketIdx, nextFreeInBucket uint8
			if !goesRight {
				preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(dst.bucketmetas))
				bucketIdx, nextFreeInBucket = leftTracker.findSlot(preferredBucketIdx, len(dst.bucketmetas), preferredBucketIdx, 0)
			} else {
				dst = right

				preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(dst.bucketmetas))
				bucketIdx, nextFreeInBucket = rightInserter.findSlot(preferredBucketIdx, len(dst.bucketmetas))
			}

			if nextFreeInBucket >= bucketSize {
				dst.ensureOverflow()
				dst.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			dstmetas := &dst.bucketmetas[bucketIdx]
			dstbuckets := &dst.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
			writeSlot(dstSlot, srcSlot)

			dst.unload--
		}
	}

	// We cleared all of the tombstones
	left.unload += int16(m.tombs)
	m.tombs = 0

	return left, right
}

// tries to shrink the map by fusing 'm' and its sibling into one map. Doesn't
// clean up the tombstones from the map that remains in use.
func (h *FishTable[K, V]) maybeShrinkByFusing(m *smolMap2[K, V], hash uintptr) (didShrink bool) {
	// Have we already shrunk as much as possible?
	if m.depth == 0 {
		return false
	}

	hibi := uintptr(1) << uintptr(m.depth-1)
	step := hibi
	siblingPos := hash & (hibi - 1)
	firstMap := h.trie[siblingPos]
	secondMap := h.trie[siblingPos+step]

	theLuckyOne := firstMap
	theUnluckyOne := secondMap

	// If our "sibling" is not on the same depth in the tree then we really
	// can't do anything shrink by other means.
	if theLuckyOne.depth != theUnluckyOne.depth {
		return false
	}

	// Remember which one is the "left" child.
	leftSibling := theLuckyOne

	// We want to hold onto the map wich has more buckets
	if cap(theUnluckyOne.bucketmetas) > cap(theLuckyOne.bucketmetas) {
		_ = sharedAllocOptimizationEnabled // for the cap()
		theLuckyOne, theUnluckyOne = theUnluckyOne, theLuckyOne
	}

	// Shrink only if the map would end up at most half full
	popSum := theLuckyOne.bucketsPop() + theUnluckyOne.bucketsPop()
	if popSum >= len(theLuckyOne.bucketmetas)*bucketSize/2 {
		// Return value is a lie but we really should prefer waiting for more
		// deletes.
		return true
	}

	// If we cross the triehash boundary we need to recalculate the hash
	crossedTriehashBoundary := (theLuckyOne.depth % triehashBoundary) == 0

	theLuckyOne.depth--

	rehash := func(outTriehash8 *uint8, isLeftChild bool) {
		// Our life is simpler when shrinking. We can take the missing bits
		// from the hash that lead us to this specific table. Except for the
		// the splitting bit, but the true value of that bit we of course can
		// figure out by knowing which table was "left" and which one is the
		// "right"

		newTrieHash := uint8(hash >> (theLuckyOne.depth / triehashBoundary * triehashBoundary))

		// Now the top (8-triehashBoundary) bits of course are wrong and need
		// correcting, the correct bits come from the old triehash value.
		//
		// So clear out the top bits and put in the correct bits from the old
		// value.
		newTrieHash = newTrieHash << (8 - triehashBoundary) >> (8 - triehashBoundary)
		newTrieHash |= (*outTriehash8 << triehashBoundary)

		// Then correct the bit that the two maps were originally split by
		whichSibling := uint8(0)
		if !isLeftChild {
			whichSibling = 1
		}
		newTrieHash &^= 1 << (triehashBoundary - 1)
		newTrieHash |= whichSibling << (triehashBoundary - 1)

		*outTriehash8 = newTrieHash
	}

	// Mark the slots that the lucky map occupies.
	var luckyTracker bucketTracker
	for bucketIndex := range theLuckyOne.bucketmetas {
		bucketmetas := &theLuckyOne.bucketmetas[bucketIndex]

		finder := bucketmetas.Finder()
		matches := finder.PresentSlots()
		luckyTracker[bucketIndex] = matches.AsBitmask()

		// Also rehash if necessary
		if crossedTriehashBoundary {
			for ; matches.HasCurrent(); matches.Advance() {
				slotInBucket := matches.Current()

				rehash(&bucketmetas.triehash8[slotInBucket], theLuckyOne == leftSibling)
			}
		}
	}

	// Move entries from the unlucky map into the lucky map
	for bucketIndex := range theUnluckyOne.bucketmetas {
		rightBucketmetas := &theUnluckyOne.bucketmetas[bucketIndex]
		rightBucket := &theUnluckyOne.buckets[bucketIndex]

		finder := rightBucketmetas.Finder()

		matches := finder.PresentSlots()

		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()
			srcSlot := makeSlotPtr(rightBucketmetas, rightBucket, slotInBucket)

			if crossedTriehashBoundary {
				rehash(srcSlot.triehash8, theUnluckyOne == leftSibling)
			}

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(theLuckyOne.bucketmetas))
			bucketIdx, nextFreeInBucket := luckyTracker.findSlot(preferredBucketIdx, len(theLuckyOne.bucketmetas), preferredBucketIdx, 0)
			if nextFreeInBucket >= bucketSize {
				theLuckyOne.ensureOverflow()
				theLuckyOne.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			leftBucketmetas := &theLuckyOne.bucketmetas[bucketIdx]
			leftBucket := &theLuckyOne.buckets[bucketIdx]
			dstSlot := makeSlotPtr(leftBucketmetas, leftBucket, nextFreeInBucket)

			// THIS IS WHERE WE MOVE IT, but beware, it could be a tombstone
			if *dstSlot.tophash8 == tophashTombstone {
				theLuckyOne.tombs--
				theLuckyOne.unload++
			}

			writeSlot(dstSlot, srcSlot)
			theLuckyOne.unload--
		}
	}

	// then the overflow
	moveOverflow := func(sm *smolMap2[K, V], ov *sortedArr[K, V]) {
		for i := range ov.hashes {
			srcSlot := makeOverflowSlotPtr(ov, i)

			if crossedTriehashBoundary {
				rehash(srcSlot.triehash8, sm == leftSibling)
			}

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(theLuckyOne.bucketmetas))
			bucketIdx, nextFreeInBucket := luckyTracker.findSlot(preferredBucketIdx, len(theLuckyOne.bucketmetas), preferredBucketIdx, 0)
			if nextFreeInBucket >= bucketSize {
				theLuckyOne.ensureOverflow()
				theLuckyOne.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			dstmetas := &theLuckyOne.bucketmetas[bucketIdx]
			dstbuckets := &theLuckyOne.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)

			// THIS IS WHERE WE MOVE IT, but beware, it could be a tombstone
			if *dstSlot.tophash8 == tophashTombstone {
				theLuckyOne.tombs--
				theLuckyOne.unload++
			}

			writeSlot(dstSlot, srcSlot)
			theLuckyOne.unload--
		}
	}

	// Try to re-slot the original overflow entries
	if ov := theLuckyOne.overflow; ov != nil {
		theLuckyOne.overflow = nil
		moveOverflow(theLuckyOne, ov)
	}

	// Then move in the overflow from the unlucky one
	if ov := theUnluckyOne.overflow; ov != nil {
		theUnluckyOne.overflow = nil
		moveOverflow(theUnluckyOne, ov)
	}

	// If the two maps were sharing the underlying buckets allocation then we
	// should zero the other half now.
	//
	// NOTE: If the other map has been doing some hardcore Deletes then it
	// has no buckets.
	if sharedAllocOptimizationEnabled {
		if cap(theLuckyOne.bucketmetas) > len(theLuckyOne.bucketmetas) && len(theUnluckyOne.bucketmetas) > 0 {
			extended := theLuckyOne.bucketmetas[:cap(theLuckyOne.bucketmetas):cap(theLuckyOne.bucketmetas)]

			// NOTE: POINTER EQUALITY. DO THE TWO POINTERS POINT TO THE SAME MEMORY?
			if &extended[len(theLuckyOne.bucketmetas)] == &theUnluckyOne.bucketmetas[0] {
				for i := range theUnluckyOne.bucketmetas {
					theUnluckyOne.bucketmetas[i] = bucketmeta{}
				}
				for i := range theUnluckyOne.buckets {
					theUnluckyOne.buckets[i] = bucket[K, V]{}
				}
			}
		}
	}

	// Update the trie
	for i := siblingPos; i < uintptr(len(h.trie)); i += step {
		h.trie[i] = theLuckyOne
	}

	// Try to shrink the trie if it's very likely that we can do so. I'm drunk
	// right now so I won't bother explaining the algorithm, it's simple enough
	// for you. You can do it!
	likelyNecessarySize := 1 << uint64(theLuckyOne.depth)
	if len(h.trie) >= 4*likelyNecessarySize {
		firstHalf := h.trie[:len(h.trie)/2]
		secondHalf := h.trie[len(h.trie)/2:]

		if len(firstHalf) != len(secondHalf) {
			panic("impossibru!")
		}

		isEqual := true
		// TODO: WHERE'S MY MEME QUAL?
		// TODO: AAAAAAAAAAAAAAA YOU FUCKS
		for i, sm := range firstHalf {
			if sm != secondHalf[i] {
				isEqual = false
				break
			}
		}

		if isEqual {
			newTrie := make([]*smolMap2[K, V], len(firstHalf))
			copy(newTrie, firstHalf)
			h.trie = newTrie
		}
	}
	return true
}

func (h *FishTable[K, V]) inplaceGrowSmol(m *smolMap2[K, V]) {
	newSize := len(m.bucketmetas) * 2
	if newSize > maxBuckets {
		newSize = maxBuckets
	}

	left := makeSmolMap2[K, V](newSize)
	left.depth = m.depth

	h.moveSmol(left, m)

	*m = *left // lol
}

func (h *FishTable[K, V]) moveSmol(dst, src *smolMap2[K, V]) {
	// Set the load with the knowledge that we will purge the tombies
	var leftInserter bucketInserter
	for bucketIndex := range src.bucketmetas {
		bucketmetas := &src.bucketmetas[bucketIndex]
		bucket := &src.buckets[bucketIndex]

		finder := bucketmetas.Finder()

		// Take the remaining slots
		matches := finder.PresentSlots()
		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()
			srcSlot := makeSlotPtr(bucketmetas, bucket, slotInBucket)

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(dst.bucketmetas))
			bucketIdx, nextFreeInBucket := leftInserter.findSlot(preferredBucketIdx, len(dst.bucketmetas))
			if nextFreeInBucket >= bucketSize {
				dst.ensureOverflow()
				dst.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			dstmetas := &dst.bucketmetas[bucketIdx]
			dstbuckets := &dst.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
			writeSlot(dstSlot, srcSlot)
			dst.unload--

			// NOTE: No need to clear the old slot, this is a move and all of
			// 'src' is supposed to soon be collected by the GC
		}
	}

	// then the overflow
	if ov := src.overflow; ov != nil {
		for i := range ov.hashes {
			srcSlot := makeOverflowSlotPtr(ov, i)

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(dst.bucketmetas))
			bucketIdx, nextFreeInBucket := leftInserter.findSlot(preferredBucketIdx, len(dst.bucketmetas))
			if nextFreeInBucket >= bucketSize {
				dst.ensureOverflow()
				dst.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			dstmetas := &dst.bucketmetas[bucketIdx]
			dstbuckets := &dst.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
			writeSlot(dstSlot, srcSlot)
			dst.unload--
		}
	}
}

func (h *FishTable[K, V]) duplicateInplace(m *smolMap2[K, V]) {
	left := makeSmolMap2[K, V](len(m.bucketmetas))
	left.depth = m.depth

	h.moveSmol(left, m)

	*m = *left // lol
}

func (h *FishTable[K, V]) rehashInplace(m *smolMap2[K, V]) {
	h.trueRehashInplace(m, 0, len(m.bucketmetas))
}

func (h *FishTable[K, V]) rehashInplaceAfterResizing(m *smolMap2[K, V], oldBucketsOffset, oldBucketsLen int) {
	h.trueRehashInplace(m, oldBucketsOffset, oldBucketsLen)
}

func (h *FishTable[K, V]) trueRehashInplace(m *smolMap2[K, V], oldBucketsOffset, oldBucketsLen int) {
	oldOverflow := m.overflow
	m.overflow = nil

	rehashingIntoSameSize := len(m.bucketmetas) == oldBucketsLen
	if len(m.bucketmetas) < oldBucketsLen {
		panic("todo")
	}

	var previousBucketHadEmpties bool
	if len(m.bucketmetas) > 1 && rehashingIntoSameSize {
		bucketmetas := &m.bucketmetas[len(m.bucketmetas)-1]
		finder := bucketmetas.Finder()
		empties := finder.EmptySlots()
		previousBucketHadEmpties = empties.HasCurrent()
	}

	var leftTracker bucketTracker
	for bucketIndex := oldBucketsOffset; bucketIndex < (oldBucketsOffset + oldBucketsLen); bucketIndex++ {
		bucketmetas := &m.bucketmetas[bucketIndex]
		bucket := &m.buckets[bucketIndex]
		trackingBucket := &leftTracker[bucketIndex]

		finder := bucketmetas.Finder()

		empties := finder.EmptySlots()
		hadEmpties := empties.HasCurrent()

		// bother to clear tombies
		if m.tombs > 0 {
			finder.MakeTombstonesIntoEmpties(bucketmetas)
		}

		matches := finder.PresentSlots()

		// If the previous bucket had empty slots before splitting, then we
		// know that all of the entries in this bucket are in their optimal
		// bucket AND that is the case quite often.
		//
		// But if we are rehashing into a larger/smaller space then that
		// assumption doesn't work.
		if previousBucketHadEmpties && rehashingIntoSameSize {
			*trackingBucket |= matches.AsBitmask()
		}

		// Don't visit entries that we know are already placed optimally
		matches = matches.WithBitmaskExcluded(*trackingBucket)

		// Move entries closer to their optimal buckets
		//
		// TODO: Use SIMD to filter out all of the entries that don't need
		// moving.
		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()
			srcSlot := makeSlotPtr(bucketmetas, bucket, slotInBucket)

			for {
				preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(m.bucketmetas))

				// If we will stay in the same bucket then we can just stay in the slot
				// that we are currently sitting in
				if uint8(bucketIndex) == preferredBucketIdx {
					// ensure that the slot is marked as taken
					trackingBucket.Mark(slotInBucket)
					break
				}

			findSlot:
				foundBucketIdx, nextFreeInBucket := leftTracker.findSlot(preferredBucketIdx, len(m.bucketmetas), uint8(bucketIndex), slotInBucket)
				if nextFreeInBucket >= bucketSize {
					// to the overflow!
					m.ensureOverflow()
					m.overflow.PutNotUpdateSlotPtr(srcSlot)
					m.unload++

					// zero the slot left behind
					zeroSlot(srcSlot)
					break
				}

				// When already tightly packed, we place into the exact same bucket
				if foundBucketIdx == uint8(bucketIndex) && nextFreeInBucket == slotInBucket {
					break
				}

				dstMetas := &m.bucketmetas[foundBucketIdx]
				dstBuckets := &m.buckets[foundBucketIdx]

				dstSlot := makeSlotPtr(dstMetas, dstBuckets, nextFreeInBucket)

				// There could be a present entry at that slot that deserves to be there
				if !isMarkerTophash(*dstSlot.tophash8) {
					victimPreferredBucket := indexIntoBuckets(*dstSlot.tophash8, len(m.bucketmetas))
					if foundBucketIdx == victimPreferredBucket {
						// Victim gets to keep its slot.
						// We will find some other slut for ourselves
						goto findSlot
					}
				}

				// Are we about to swap places with a tombstone?
				if *dstSlot.tophash8 == tophashTombstone {
					*dstSlot.tophash8 = tophashEmpty
				}

				// swap, nicely zeroes the slot left behind if we are inserting into an
				// empty slot
				swapSlots(dstSlot, srcSlot)

				// Did we swap in an empty slot or a present one?
				if *srcSlot.tophash8 == tophashEmpty {
					break
				}
			}
		}

		previousBucketHadEmpties = hadEmpties
	}

	// Then the overflows :-)
	if ov := oldOverflow; ov != nil {
		for i := range ov.hashes {
			srcSlot := makeOverflowSlotPtr(ov, i)

			preferredBucketIdx := indexIntoBuckets(*srcSlot.tophash8, len(m.bucketmetas))
			bucketIdx, nextFreeInBucket := leftTracker.findSlot(preferredBucketIdx, len(m.bucketmetas), preferredBucketIdx, 0)
			if nextFreeInBucket >= bucketSize {
				m.ensureOverflow()
				m.overflow.PutNotUpdateSlotPtr(srcSlot)
				continue
			}

			dstmetas := &m.bucketmetas[bucketIdx]
			dstbuckets := &m.buckets[bucketIdx]
			dstSlot := makeSlotPtr(dstmetas, dstbuckets, nextFreeInBucket)
			writeSlot(dstSlot, srcSlot)
			m.unload--
		}
	}

	// We did also clear all of the tombstones
	m.unload += int16(m.tombs)
	m.tombs = 0
}

func (m *FishTable[K, V]) iterateMaps(iter func(*smolMap2[K, V]) bool) {
	// Oh... god... modifying iterators... pain...
	for i := range m.trie {
		triehash := uint64(i)
		sm := m.trie[triehash]

		// Did we visit this map already? Eerily similar to the logic elsewhere ;)
		hibi := uint64(1) << uint64(sm.depth)
		if triehash != (triehash & (hibi - 1)) {
			continue
		}

		ok := iter(sm)
		if !ok {
			return
		}
	}
}

// Iterates over all of the key-value pairs in the map. Modifying during
// iteration is allowed.
//
// If a map entry that has not yet been reached is removed during iteration, the
// corresponding iteration value will not be produced. If a map entry is created
// during iteration, that entry may be produced during the iteration or may be
// skipped. The choice may vary for each entry created and from one iteration to
// the next.
//
// NOTE: The above semantics are the goal. We are not there yet. Don't modify
// while iterating for now.
func (m *FishTable[K, V]) Iterate(iter func(K, V) bool) {
	// NOTE: Not random order
	m.iterateMaps(func(sm *smolMap2[K, V]) bool {
		for i := range sm.buckets {
			bucketmetas := &sm.bucketmetas[i]
			bucket := &sm.buckets[i]

			finder := bucketmetas.Finder()
			hashMatches := finder.PresentSlots()
			for ; hashMatches.HasCurrent(); hashMatches.Advance() {
				idx := hashMatches.Current()

				if ok := iter(bucket.keys[idx], bucket.values[idx]); !ok {
					return false
				}
			}
		}

		if ov := sm.overflow; ov != nil {
			for i := range ov.keys {
				if ok := iter(ov.keys[i], ov.values[i]); !ok {
					return false
				}
			}
		}

		return true
	})
}

func makeSmolMap2[K comparable, V any](buckets int) *smolMap2[K, V] {
	// TODO: Interesting thought would be to allocate precisely to some
	// allocation size-class. Or maybe limit the allocation to the page size.
	// What happens when all of the hashmaps in the software do allocations of
	// the same size, no matter what the K and V types are for those maps?
	if buckets > maxBuckets {
		println(buckets)
		panic("bad size")
	}

	m := new(smolMap2[K, V])
	m.bucketmetas = make([]bucketmeta, buckets)
	m.buckets = make([]bucket[K, V], buckets)
	m.unload = m.maxLoad()
	return m
}

func (m *FishTable[K, V]) load() (occupied, totalSlots uint64) {
	m.iterateMaps(func(m *smolMap2[K, V]) bool {
		for bucketIndex := range m.bucketmetas {
			bucketmetas := &m.bucketmetas[bucketIndex]

			totalSlots += uint64(len(bucketmetas.tophash8))

			finder := bucketmetas.Finder()
			presentSlots := finder.PresentSlots()
			occupied += uint64(presentSlots.Count())
		}

		if m.overflow != nil {
			totalSlots += uint64(cap(m.overflow.hashes))
			occupied += uint64(m.overflow.Len())
		}
		return true
	})
	return occupied, totalSlots
}

func (m *FishTable[K, V]) loadFactor() float64 {
	occupied, totalSlots := m.load()
	return float64(occupied) / float64(totalSlots)
}

func (m *smolMap2[K, V]) capacity() int {
	return len(m.bucketmetas) * bucketSize
}

func (m *smolMap2[K, V]) maxLoad() int16 {
	return int16(len(m.bucketmetas) * bucketSize * lfnumerator / lfdenominator)
}

func (m *smolMap2[K, V]) bucketsPop() int {
	unloadMissing := m.capacity() - int(m.maxLoad())
	return m.capacity() - (int(m.unload) + unloadMissing) - int(m.tombs)
}

func (m *smolMap2[K, V]) ensureOverflow() {
	if m.overflow == nil {
		m.overflow = new(sortedArr[K, V])
	}
}

type hashpair struct {
	tophash8  tophash
	triehash8 uint8
}

// slice of key-value pairs sorted by tophash.
//
// Starts to die after >256 kv-pairs added. But that should happen only when
// the hash function shits the bed exceptionally bad. Usually len(keys)<= 8 but
// sometimes len(keys)<=32.
type sortedArr[K comparable, V any] struct {
	hashes []hashpair
	keys   []K
	values []V
}

func (a *sortedArr[K, V]) Len() int {
	if a == nil {
		return 0
	}
	return len(a.hashes)
}

func (a *sortedArr[K, V]) Delete(key K, tophash8 tophash, triehash8 uint8) bool {
	for i, e := range a.hashes {
		if tophash8 < e.tophash8 {
			break
		}
		if tophash8 == e.tophash8 && e.triehash8 == triehash8 && a.keys[i] == key {
			a.hashes = append(a.hashes[:i], a.hashes[i+1:]...)
			a.keys = append(a.keys[:i], a.keys[i+1:]...)
			a.values = append(a.values[:i], a.values[i+1:]...)
			return true
		}
	}
	return false
}

func (a *sortedArr[K, V]) Get(key K, tophash8 tophash, triehash8 uint8) (V, bool) {
	for i, e := range a.hashes {
		if tophash8 < e.tophash8 {
			break
		}
		if tophash8 == e.tophash8 && e.triehash8 == triehash8 && a.keys[i] == key {
			return a.values[i], true
		}
	}
	var zerov V
	return zerov, false
}

func (a *sortedArr[K, V]) Put(key K, value V, tophash8 tophash, triehash8 uint8) bool {
	where := len(a.hashes)
	for i, e := range a.hashes {
		if tophash8 < e.tophash8 {
			where = i
			break
		}
		if tophash8 == e.tophash8 && e.triehash8 == triehash8 && a.keys[i] == key {
			a.values[i] = value
			return true
		}
	}

	a.hashes = append(a.hashes, hashpair{})
	a.keys = append(a.keys, key)
	a.values = append(a.values, value)
	copy(a.hashes[where+1:], a.hashes[where:])
	copy(a.keys[where+1:], a.keys[where:])
	copy(a.values[where+1:], a.values[where:])
	a.hashes[where] = hashpair{tophash8, triehash8}
	a.keys[where] = key
	a.values[where] = value
	return false
}

// Puts the entry into the array assuming that there can't be another entry
// with the same key.
func (a *sortedArr[K, V]) PutNotUpdateSlotPtr(slot slotPtr[K, V]) {
	where := len(a.hashes)
	for i, e := range a.hashes {
		if *slot.tophash8 < e.tophash8 {
			where = i
			break
		}
	}

	a.hashes = append(a.hashes, hashpair{})
	a.keys = append(a.keys, *slot.key)
	a.values = append(a.values, *slot.value)
	copy(a.hashes[where+1:], a.hashes[where:])
	copy(a.keys[where+1:], a.keys[where:])
	copy(a.values[where+1:], a.values[where:])
	a.hashes[where] = hashpair{*slot.tophash8, *slot.triehash8}
	a.keys[where] = *slot.key
	a.values[where] = *slot.value
}

func (a *sortedArr[K, V]) TryToMoveBackToBucket(metas *bucketmeta, bucket *bucket[K, V], bucketIndex, slotInBucket uint8, numBuckets int) bool {
	// oof
	for i, e := range a.hashes {
		preferredBucketIdx := indexIntoBuckets(e.tophash8, numBuckets)
		for j := 1; j <= maxProbeDistance; j++ {
			if preferredBucketIdx == bucketIndex {
				// do the move
				metas.tophash8[slotInBucket] = e.tophash8
				metas.triehash8[slotInBucket] = e.triehash8
				bucket.keys[slotInBucket] = a.keys[i]
				bucket.values[slotInBucket] = a.values[i]

				a.hashes = append(a.hashes[:i], a.hashes[i+1:]...)
				a.keys = append(a.keys[:i], a.keys[i+1:]...)
				a.values = append(a.values[:i], a.values[i+1:]...)
				return true
			}
			preferredBucketIdx = nextIndexIntoBuckets(preferredBucketIdx, numBuckets)
		}
	}
	return false
}
