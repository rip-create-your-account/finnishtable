package finnishtable

import (
	"math"
	"math/bits"
	_ "unsafe"
)

//go:linkname runtime_fastrand64 runtime.fastrand64
func runtime_fastrand64() uint64

const (
	// Maximum number of buckets per map.
	// NOTE: Must be a power of two
	// NOTE: With our hashing scheme we can have up to 256 buckets per map.
	maxBuckets = 32

	maxEntriesPerMap = maxBuckets * bucketSize
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
	return hash & uint8(numBuckets-1)
}

// A Finnish hash table, or fish for short. Stores key-value pairs.
type FishTable[K comparable, V any] struct {
	hasher func(k K, mapseed uint64) uint64
	trie   []*smolMap2[K, V] // TODO: Maybe store (smolMap.depth) here to get better use of the cache-lines
	seed   uint64
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
//
// WARNING: Put() never returns if more than (maxBuckets*bucketSize = 512) keys
// have the exact same hash value.
func MakeFishTable[K comparable, V any](hasher func(key K, seed uint64) uint64) *FishTable[K, V] {
	m := new(FishTable[K, V])
	m.hasher = hasher

	// The funny thing with random seed is that it makes cloning via
	// m.Iterate() slower by a good amount
	m.seed = runtime_fastrand64()

	// TODO: sizehint to pre-allocate the trie to a size where we will not need
	// to rehash shit too much. The actual maps could be allocated just-in-time
	// when inserting.
	//
	// numMapsPerSizeHint := 1 << bits.Len(sizehint/256 ???)
	// m.trie = make([]*smolMap2[K, V], numMapsPerSizeHint)

	return m
}

type smolMap2[K comparable, V any] struct {
	// TODO: Make it so that the number of buckets in a map doesn't need to be
	// a power of two. I suppose the max number of buckets needs to stay as
	// 256. The purpose would be to allow allocating such a count of buckets
	// that the space required is a "neat" number no matter the K and V types.
	// Such that one could try to match the system page size or some other
	// target.
	bucketmetas []bucketmeta
	buckets     []bucket[K, V]

	depth      uint8 // <= 64
	preferGrow bool
	pop        uint16 // <= maxEntriesPerMap
	tombs      uint16 // <= maxEntriesPerMap
}

func (m *FishTable[K, V]) Len() int {
	return m.len
}

func (m *FishTable[K, V]) Get(k K) (V, bool) {
	if len(m.trie) > 0 {
		hash := m.hasher(k, m.seed)

		tophash8 := fixTophash(uint8(hash >> 56))
		triehash8 := uint8(hash >> 0)

		sm := m.trie[hash&uint64(len(m.trie)-1)]

		mapmetas := sm.bucketmetas
		if len(mapmetas) == 0 { // nil if we shrunk very hard
			goto miss
		}

		if len(m.trie) > 128 {
			triehash8 = uint8(hash >> (sm.depth / 8 * 8)) // NOTE: Humbug! For large tries this is always a cache-miss
		}

		tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
		bucketIndex := indexIntoBuckets(tophash8, len(mapmetas)) // start looking from tophash8 location
		max := bucketIndex                                       // loop around once

		for {
			bucketmetas := &mapmetas[bucketIndex]
			finder := bucketmetas.Finder()

			slotsToLookAt := finder.ProbeHashMatches(tophashProbe, triehashProbe)
			for ; slotsToLookAt.HasCurrent(); slotsToLookAt.Advance() {
				idx := slotsToLookAt.Current()

				bucket := &sm.buckets[bucketIndex]
				if bucket.keys[idx] == k {
					// TODO: If we ended up wading through a wast sea of
					// buckets, we could consider doing a split to hopefully
					// make future lookups faster by not having so full of a
					// map.
					return bucket.values[idx], true
				}

				// TODO: If we get lots of false positive matches, it could be
				// because our triehash is not being useful to us. If the depth
				// of sm is for example 15, then it means that the 7 lowest
				// bits of the triehash are exactly the same between the
				// entries of sm and only the top bit differs. We could just do
				// a split here if we see too many false positives. Same
				// applies for Put() and Delete(). Sounds silly.
			}

			empties := finder.EmptySlots()
			if empties.HasCurrent() {
				// No need to look further - the probing during inserting would
				// have placed the key into this slot.
				goto miss
			}

			bucketIndex = (bucketIndex + 1) & uint8(len(mapmetas)-1)
			if bucketIndex == max {
				goto miss
			}
		}
	}

miss:
	var zerov V
	return zerov, false
}

func (m *FishTable[K, V]) Delete(k K) {
	if len(m.trie) == 0 {
		return
	}

	hash := m.hasher(k, m.seed)

	sm := m.trie[hash&uint64(len(m.trie)-1)]

	tophash8 := fixTophash(uint8(hash >> 56))
	triehash8 := uint8(hash >> 0)

	mapmetas := sm.bucketmetas
	if len(mapmetas) == 0 { // nil if we shrunk very hard
		return
	}

	if len(m.trie) > 128 {
		triehash8 = uint8(hash >> (sm.depth / 8 * 8)) // NOTE: Humbug! For large tries this is always a cache-miss
	}

	tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
	bucketIndex := uint(indexIntoBuckets(tophash8, len(mapmetas))) // start looking from tophash8 location
	max := bucketIndex                                             // loop around once

	for {
		bucketmetas := &mapmetas[bucketIndex]
		finder := bucketmetas.Finder()

		slotsToLookAt := finder.ProbeHashMatches(tophashProbe, triehashProbe)
		for ; slotsToLookAt.HasCurrent(); slotsToLookAt.Advance() {
			slotInBucket := slotsToLookAt.Current()

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
				empties := finder.EmptySlots()
				if empties.HasCurrent() {
					bucketmetas.tophash8[slotInBucket] = tophashEmpty
				} else {
					bucketmetas.tophash8[slotInBucket] = tophashTombstone
					sm.tombs++
				}

				bucketmetas.triehash8[slotInBucket] = 0
				sm.pop--
				m.len--

				// Shrink if 1/4 full.
				// TODO: More thoughtful shrinking. As a starting point we at
				// least reduce the number of buckets to match the number of
				// entries that remain in the map.
				if sm.pop <= uint16(len(sm.bucketmetas)*(bucketSize/4)) {
					// When empty, release the buckets
					// TODO: Shrink the trie? Some other shrinking strategy?
					if sm.pop == 0 {
						sm.bucketmetas = nil
						sm.buckets = nil
					} else if len(sm.bucketmetas) > 1 {
						m.inplaceShrinkSmol(sm)
						sm.preferGrow = true
					}
				}

				return
			}
		}

		empties := finder.EmptySlots()
		if empties.HasCurrent() {
			// No need to look further - the probing during inserting would
			// have placed the key into this slot.
			return
		}

		bucketIndex = (bucketIndex + 1) & uint(len(mapmetas)-1)
		if bucketIndex == max {
			return
		}
	}
}

// Does the Put thing.
func (m *FishTable[K, V]) Put(k K, v V) {
	hash := m.hasher(k, m.seed)
	if len(m.trie) == 0 {
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
		sm.pop++
		m.len++
		return
	}

top:
	sm := m.trie[hash&uint64(len(m.trie)-1)]

	tophash8 := fixTophash(uint8(hash >> 56))
	triehash8 := uint8(hash >> 0)

	mapmetas := sm.bucketmetas
	if len(mapmetas) == 0 { // nil if we shrunk very hard
		sm.bucketmetas = make([]bucketmeta, 1)
		sm.buckets = make([]bucket[K, V], 1)
		mapmetas = sm.bucketmetas
	}

	if len(m.trie) > 128 {
		triehash8 = uint8(hash >> (sm.depth / 8 * 8)) // NOTE: Humbug! For large tries this is always a cache-miss
	}

	tophashProbe, triehashProbe := makeHashProbes(tophash8, triehash8)
	bucketIndex := uint(indexIntoBuckets(tophash8, len(mapmetas))) // start looking from tophash8 location
	max := bucketIndex + uint(len(mapmetas))                       // loop around once
	bucketMask := uint(len(mapmetas) - 1)

	for {
		bucketmetas := &mapmetas[bucketIndex&bucketMask]
		finder := bucketmetas.Finder()

		hashMatches := finder.ProbeHashMatches(tophashProbe, triehashProbe)
		for ; hashMatches.HasCurrent(); hashMatches.Advance() {
			idx := hashMatches.Current()

			bucket := &sm.buckets[bucketIndex&bucketMask]
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
		empties := finder.EmptySlots()
		if empties.HasCurrent() {
			idx := empties.Current()

			// TODO: If we saw a tombstone along the way then we could also
			// take that spot. But do I care to? NO!

			bucket := &sm.buckets[bucketIndex&bucketMask]
			bucket.keys[idx] = k
			bucket.values[idx] = v
			bucketmetas.tophash8[idx] = tophash8
			bucketmetas.triehash8[idx] = triehash8
			sm.pop++
			m.len++

			// Check if we should grow. It's good to do it now that we quite
			// likely probed through a good chunk of the map. Surely data is
			// now in the CPU caches.

			// Don't start enforcing max load factor if probing through the
			// whole thing wouldn't be too bad. But if we are full, just do it
			//
			// Chosen limits here are RNG generated (by a state-of-the-art RNG)
			maxLoad := uint16(len(mapmetas)*bucketSize) - 1
			if len(mapmetas) <= 2 {
				// For small maps let's just force the next insert to probe
				// through the whole shit. We get to defer the allocation and
				// life is kinda good.
				// TODO: Remove?
				maxLoad = math.MaxUint16
			} else if len(mapmetas) > 4 {
				// Chosen load factor limit is RNG generated.
				maxLoad = uint16(len(mapmetas) * (bucketSize * 9 / 10))
			}
			if sm.pop+sm.tombs <= maxLoad {
				return
			}

			m.makeSpaceForMap(sm, hash)
			return
		}

		bucketIndex++
		if bucketIndex == max {
			// THIS IS WHERE WE GROW THEM!
			m.makeSpaceForMap(sm, hash)
			goto top
		}
	}
}

func (m *FishTable[K, V]) makeSpaceForMap(sm *smolMap2[K, V], hash uint64) {
	// Just rehash if map has tombstones
	if sm.tombs > 0 {
		rehashInplace(m, sm)
		return
	}

	// Instead of always splitting, we just add some more buckets to the map.
	// Also, if map has shrunk a lot after Delete()s we want to grow instead of
	// splitting. Also it's nice to try to milk as much as possible out of the
	// initial triehash8.
	reallyShouldPreferGrow := sm.depth >= 7
	if (reallyShouldPreferGrow || sm.preferGrow) && len(sm.bucketmetas) < maxBuckets {
		m.inplaceGrowSmol(sm)
		sm.preferGrow = false // split next time
		return
	}

	// split it
	oldDepth := sm.depth
	left, right := m.split(sm)

	// add more buckets next time
	left.preferGrow = true
	right.preferGrow = true

	// But maybe the trie needs to grow as well...
	if 1<<oldDepth == len(m.trie) {
		oldTrie := m.trie
		m.trie = make([]*smolMap2[K, V], len(oldTrie)*2)
		copy(m.trie[:len(oldTrie)], oldTrie)
		copy(m.trie[len(oldTrie):], oldTrie)
	}

	hibi := uint64(1) << uint64(oldDepth)
	step := hibi
	for i := uint64(hash & (hibi - 1)); i < uint64(len(m.trie)); i += step {
		if i&hibi == 0 {
			// Avoid touching memory that we don't need to touch. Saving 1
			// cache line is totally worth it! Implying that the CPU doesn't
			// load it anyways.
			if left != sm {
				m.trie[i] = left
			}
		} else {
			m.trie[i] = right
		}
	}
}

type bucketInserter [maxBuckets]uint8

func (freeSlots *bucketInserter) findSlot(preferredBucketIdx uint8, numBuckets int) (uint8, uint8) {
	idxMask := uint8(numBuckets - 1)
	bucketIdx := preferredBucketIdx
	max := (bucketIdx + uint8(numBuckets)) & idxMask
	for {
		nextFreeInBucket := freeSlots[bucketIdx]
		if nextFreeInBucket >= bucketSize {
			bucketIdx = (bucketIdx + 1) & idxMask
			if bucketIdx == max {
				panic("how?")
			}
			continue
		}
		freeSlots[bucketIdx]++
		return bucketIdx, nextFreeInBucket
	}
}

type bucketTracker [maxBuckets][bucketSize]uint8

func (tracker *bucketTracker) findSlot(preferredBucketIdx uint8, numBuckets int, unfortunateBucket, unfortunateSlot uint8) (uint8, uint8) {
	idxMask := uint8(numBuckets - 1)
	bucketIdx := preferredBucketIdx
	max := (bucketIdx + uint8(numBuckets)) & idxMask
	for {
		takeable := findZeros(&tracker[bucketIdx])
		if !takeable.HasCurrent() {
			bucketIdx = (bucketIdx + 1) & idxMask
			if bucketIdx == max {
				panic("how?")
			}
			continue
		}

		// If worst comes to worst, we landed all the way into the unfortunate
		// bucket. Try to prefer the given slot.
		if bucketIdx == unfortunateBucket && tracker[bucketIdx][unfortunateSlot] == 0 {
			tracker[bucketIdx][unfortunateSlot] = 1
			return bucketIdx, unfortunateSlot
		}

		nextFreeInBucket := takeable.Current()
		tracker[bucketIdx][nextFreeInBucket] = 1
		return bucketIdx, nextFreeInBucket
	}
}

// places the entry as close as possible to it's preferred bucket. Returns true
// if it swapped places with some other entry, and now that entry will need
// placing himself.
func placeTightly[K comparable, V any](sm *smolMap2[K, V], bucketmetas *bucketmeta, bucket *bucket[K, V], slotInBucket, currentBucketIdx uint8, freeSlots *bucketTracker) bool /*swapped*/ {
	preferredBucketIdx := indexIntoBuckets(bucketmetas.tophash8[slotInBucket], len(sm.bucketmetas))

	// If we will stay in the same map, could we also stay in our current
	// slot?
	if preferredBucketIdx == currentBucketIdx {
		if freeSlots[preferredBucketIdx][slotInBucket] == 0 {
			freeSlots[preferredBucketIdx][slotInBucket] = 1 // mark as taken
			sm.pop++
			return false
		}
	}

	bucketIdx, nextFreeInBucket := freeSlots.findSlot(preferredBucketIdx, len(sm.bucketmetas), currentBucketIdx, slotInBucket)

	dstMetas := &sm.bucketmetas[bucketIdx]
	dstBuckets := &sm.buckets[bucketIdx]
	if !isMarkerTophash(dstMetas.tophash8[nextFreeInBucket]) {
		// When already tightly packed, we place into the exact same bucket
		if bucketIdx == currentBucketIdx && nextFreeInBucket == slotInBucket {
			sm.pop++
			return false
		}

		// swap
		dstMetas.tophash8[nextFreeInBucket], bucketmetas.tophash8[slotInBucket] = bucketmetas.tophash8[slotInBucket], dstMetas.tophash8[nextFreeInBucket]
		dstMetas.triehash8[nextFreeInBucket], bucketmetas.triehash8[slotInBucket] = bucketmetas.triehash8[slotInBucket], dstMetas.triehash8[nextFreeInBucket]
		dstBuckets.keys[nextFreeInBucket], bucket.keys[slotInBucket] = bucket.keys[slotInBucket], dstBuckets.keys[nextFreeInBucket]
		dstBuckets.values[nextFreeInBucket], bucket.values[slotInBucket] = bucket.values[slotInBucket], dstBuckets.values[nextFreeInBucket]

		sm.pop++
		return true
	}

	dstMetas.tophash8[nextFreeInBucket] = bucketmetas.tophash8[slotInBucket]
	dstMetas.triehash8[nextFreeInBucket] = bucketmetas.triehash8[slotInBucket]
	dstBuckets.keys[nextFreeInBucket] = bucket.keys[slotInBucket]
	dstBuckets.values[nextFreeInBucket] = bucket.values[slotInBucket]

	// zero the slot left behind
	var zerok K
	var zerov V
	bucket.keys[slotInBucket] = zerok
	bucket.values[slotInBucket] = zerov
	bucketmetas.tophash8[slotInBucket] = tophashEmpty
	bucketmetas.triehash8[slotInBucket] = 0

	sm.pop++
	return false
}

func (h *FishTable[K, V]) split(m *smolMap2[K, V]) (*smolMap2[K, V], *smolMap2[K, V]) {
	if m.depth == 64 {
		// 64-bit hash values have only 64 bits :-(
		panic("depth overflow")
	}

	m.depth++

	// Re-use m as left
	left := m
	left.pop = 0
	left.depth = m.depth

	right := makeSmolMap2[K, V](len(m.bucketmetas))
	right.depth = m.depth

	oldDepthBit := uint8(1 << ((right.depth - 1) % 8))
	isAt8Boundary := m.depth&(8-1) == 0

	{
		// Move all applicable entries to the "right" map and clear tombstones
		// as we do.

		var rightInserter bucketInserter
		for bucketIndex := 0; bucketIndex < len(m.bucketmetas); bucketIndex++ {
			bucketmetas := &m.bucketmetas[bucketIndex]
			bucket := &m.buckets[bucketIndex]

			finder := bucketmetas.Finder()

			// bother to clear tombies if we are planning on reusing m
			if left == m && left.tombs > 0 {
				finder.MakeTombstonesIntoEmpties(bucketmetas)
			}

			matches := finder.PresentSlots()
			for ; matches.HasCurrent(); matches.Advance() {
				slotInBucket := matches.Current()

				goesRight := bucketmetas.triehash8[slotInBucket]&oldDepthBit > 0

				if isAt8Boundary {
					hash := h.hasher(bucket.keys[slotInBucket], h.seed)
					triehash8 := uint8(hash >> (m.depth / 8 * 8))
					bucketmetas.triehash8[slotInBucket] = triehash8
				}

				if !goesRight {
					continue
				}

				preferredBucketIdx := indexIntoBuckets(bucketmetas.tophash8[slotInBucket], len(right.bucketmetas))
				bucketIdx, nextFreeInBucket := rightInserter.findSlot(preferredBucketIdx, len(right.bucketmetas))

				right.bucketmetas[bucketIdx].tophash8[nextFreeInBucket] = bucketmetas.tophash8[slotInBucket]
				right.bucketmetas[bucketIdx].triehash8[nextFreeInBucket] = bucketmetas.triehash8[slotInBucket]
				right.buckets[bucketIdx].keys[nextFreeInBucket] = bucket.keys[slotInBucket]
				right.buckets[bucketIdx].values[nextFreeInBucket] = bucket.values[slotInBucket]
				right.pop++

				// Zero the slot left behind. Only setting the tophash8 to
				// empty is strictly necessary here so we don't mess up the
				// "left" part.
				var zerok K
				var zerov V
				bucket.keys[slotInBucket] = zerok
				bucket.values[slotInBucket] = zerov
				bucketmetas.tophash8[slotInBucket] = tophashEmpty
				bucketmetas.triehash8[slotInBucket] = 0
			}
		}

		left.tombs = 0
	}

	if m != left {
		h.moveSmol(left, m)
		return left, right
	}

	// For the remaining entries in 'left' we need to pack them tightly, each
	// entry as close as possible to it's optimal bucket

	var leftTracker bucketTracker
	for bucketIndex := 0; bucketIndex < len(m.bucketmetas); bucketIndex++ {
		bucketmetas := &m.bucketmetas[bucketIndex]
		bucket := &m.buckets[bucketIndex]

		finder := bucketmetas.Finder()
		matches := finder.PresentSlots()
		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()

			// Was this key already looked for? If the slot is no longer free
			// then we know that earlier we were placed it here into this
			// current slot.
			if leftTracker[bucketIndex][slotInBucket] != 0 {
				continue
			}

			for {
				swapped := placeTightly(m, bucketmetas, bucket, uint8(slotInBucket), uint8(bucketIndex), &leftTracker)
				if !swapped {
					break
				}
			}
		}
	}

	return left, right
}

func (h *FishTable[K, V]) inplaceGrowSmol(m *smolMap2[K, V]) {
	newSize := len(m.bucketmetas) * 2

	left := makeSmolMap2[K, V](newSize)
	left.depth = m.depth

	h.moveSmol(left, m)

	*m = *left // lol
}

func (h *FishTable[K, V]) inplaceShrinkSmol(m *smolMap2[K, V]) {
	newSize := len(m.bucketmetas) / 2

	left := makeSmolMap2[K, V](newSize)
	left.depth = m.depth

	h.moveSmol(left, m)

	*m = *left // lol
}

func (h *FishTable[K, V]) moveSmol(dst, src *smolMap2[K, V]) {
	var leftInserter bucketInserter
	for bucketIndex := 0; bucketIndex < len(src.bucketmetas); bucketIndex++ {
		bucketmetas := &src.bucketmetas[bucketIndex]
		bucket := &src.buckets[bucketIndex]

		finder := bucketmetas.Finder()

		// Take the remaining slots
		matches := finder.PresentSlots()
		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()

			preferredBucketIdx := indexIntoBuckets(bucketmetas.tophash8[slotInBucket], len(dst.bucketmetas))
			bucketIdx, nextFreeInBucket := leftInserter.findSlot(preferredBucketIdx, len(dst.bucketmetas))

			dst.bucketmetas[bucketIdx].tophash8[nextFreeInBucket] = bucketmetas.tophash8[slotInBucket]
			dst.bucketmetas[bucketIdx].triehash8[nextFreeInBucket] = bucketmetas.triehash8[slotInBucket]
			dst.buckets[bucketIdx].keys[nextFreeInBucket] = bucket.keys[slotInBucket]
			dst.buckets[bucketIdx].values[nextFreeInBucket] = bucket.values[slotInBucket]
			dst.pop++

			// NOTE: No need to clear the old slot, we do it after the loop
		}
	}

	// zero the pointers
	// TODO: This kills the iterator
	for i := range src.buckets {
		src.buckets[i] = bucket[K, V]{}
	}
}

func rehashInplace[K comparable, V any](h *FishTable[K, V], m *smolMap2[K, V]) {
	m.pop = 0 // placeTightly increments pop for every entry

	var tracker bucketTracker
	for bucketIndex := 0; bucketIndex < len(m.buckets); bucketIndex++ {
		bucketmetas := &m.bucketmetas[bucketIndex]
		bucket := &m.buckets[bucketIndex]

		finder := bucketmetas.Finder()

		// clear tombies
		if m.tombs > 0 {
			finder.MakeTombstonesIntoEmpties(bucketmetas)
		}

		matches := finder.PresentSlots()
		for ; matches.HasCurrent(); matches.Advance() {
			slotInBucket := matches.Current()

			// Was this key already looked for? If the slot is no longer free
			// then we know that earlier we were placed it here into this
			// current slot.
			if tracker[bucketIndex][slotInBucket] != 0 {
				continue
			}

			for {
				swapped := placeTightly(m, bucketmetas, bucket, uint8(slotInBucket), uint8(bucketIndex), &tracker)
				if !swapped {
					break
				}
			}
		}
	}

	// Tombies were killed in the above loop.
	m.tombs = 0
}

func (m *FishTable[K, V]) iterateMaps(iter func(*smolMap2[K, V]) bool) {
	// Oh... god... modifying iterators... pain...
	for i := range m.trie {
		triehash := uint64(i)
		sm := m.trie[triehash]

		// Did we visit this map already? Eerily similar to the logic in
		// dosplit ;)
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
		return true
	})
}

func makeSmolMap2[K comparable, V any](buckets int) *smolMap2[K, V] {
	// TODO: Interesting thought would be to allocate precisely to some
	// allocation size-class. Or maybe limit the allocation to the page size.
	// What happens when all of the hashmaps in the software do allocations of
	// the same size, no matter what the K and V types are for those maps?
	if buckets > maxBuckets || bits.OnesCount(uint(buckets)) != 1 {
		println(buckets)
		panic("bad size")
	}

	m := new(smolMap2[K, V])
	m.bucketmetas = make([]bucketmeta, buckets)
	m.buckets = make([]bucket[K, V], buckets)
	return m
}

func (m *FishTable[K, V]) load() (occupied, totalSlots uint64) {
	seen := make(map[*smolMap2[K, V]]struct{}, len(m.trie))
	for i := 0; i < len(m.trie); i++ {
		seen[m.trie[i]] = struct{}{}
	}
	for m := range seen {
		for bucketIndex := range m.bucketmetas {
			bucketmetas := &m.bucketmetas[bucketIndex]

			totalSlots += uint64(len(bucketmetas.tophash8))

			finder := bucketmetas.Finder()
			presentSlots := finder.PresentSlots()
			occupied += uint64(presentSlots.Count())

		}
	}
	return occupied, totalSlots
}
