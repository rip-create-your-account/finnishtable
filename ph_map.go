package finnishtable

import (
	"math/bits"
	"unsafe"

	"github.com/rip-create-your-account/finnishtable/asm"
)

// TODO: Specialize fixTophash for ph hash. We have no tombstones so only 0 is
// reserved as a special value. That would require modifying my lovely assembly
// routines so I won't bother for now.

type kv[K comparable, V any] struct {
	key   K
	value V
}

type phtrieEntry struct {
	// Really trie entries need to only carry the bucketsOffset but I decided
	// to hide the hash rotation here as well just because it fits here so
	// smoothly
	_bucketsOffset_hashrot uint32 // 26b + 6b
}

func (e *phtrieEntry) bucketsOffset() uint32 {
	return e._bucketsOffset_hashrot >> 6
}

func (e *phtrieEntry) hashrot() uint32 {
	// So... bits.RotateLeft only looks at the lower 6 bits. So basically I'm
	// just gonna not mask off the upper bits. It's kinda nice that
	// bucketsOffset is just a shr instruction and this one is nothing.
	return e._bucketsOffset_hashrot
}

type phbucketmeta struct {
	// top 8-bits of the hash adjusted with fixTophash()
	tophash8 [bucketSize]tophash
}

type phbucket struct {
	phbucketmeta
	kvsOffset uint32
}

type phbucketfinder struct{ *phbucketmeta }

func (b *phbucketmeta) Finder() phbucketfinder {
	return phbucketfinder{b}
}

var zeroes16 [16]uint8

func (b *phbucketfinder) ProbeHashMatchesAndEmpties(tophashProbe tophashprobe) (hashes, empties matchiter) {
	hashes.hashMatches, empties.hashMatches = asm.FindHashesAndEmpties(&b.tophash8, &zeroes16, uint8(tophashProbe), 0)
	return
}

func (b *phbucketfinder) PresentSlots() matchiter {
	hashMatches := asm.FindBytesWhereBitsRemainSetAfterApplyingThisMask(&b.tophash8, 0b0111_1111)
	return matchiter{hashMatches: hashMatches}
}

// A perfect hashing Finnish hash table, or phfish for short. Stores key-value pairs.
type PhFishTable[K comparable, V any] struct {
	hasher func(key unsafe.Pointer, seed uintptr) uintptr
	trie   []phtrieEntry

	// TODO: In a decent language you could have the metas and kvs be
	// interleaved into a single array and have the trie point to the start of
	// the metas for that block. It would also make the phbuckets smaller as
	// they wouldn't need to carry a pointer to the kvs.
	//
	// eg. {[16]byte, kv0, kv1, kv2, kv3, kv4, [16]byte, kv0, kv1, [16]byte,
	// kv0, kv1, kv2, ... etc}
	//
	// Less cache misses all around, l1, l2 ... tlb, etc...
	allKvs   []kv[K, V]
	allMetas []phbucket

	seed uintptr
}

func (m *PhFishTable[K, V]) Len() int {
	return len(m.allKvs)
}

func (m *PhFishTable[K, V]) Get(k K) (V, bool) {
	trie := m.trie
	if len(trie) > 0 {
		hash := m.hasher(noescape(unsafe.Pointer(&k)), m.seed)

		sm := &trie[hash&uintptr(len(trie)-1)]

		rotatedHash := bits.RotateLeft(uint(hash), -int(sm.hashrot()))
		tophash8 := fixTophash(uint8(rotatedHash))

		tophashProbe, _ := makeHashProbes(tophash8, 0)

		bucketmetas := &m.allMetas[sm.bucketsOffset()]
		kvsOffset := bucketmetas.kvsOffset

		// NOTE: The nice thing about having to find the matching tophash is
		// the case when there's none - because the key is not in the map! So
		// with a good likelihood we don't have to waste our time comparing the
		// keys. Goddamn is this shit practical and nice and stuff.
		finder := bucketmetas.Finder()
		hashMatches, _ := finder.ProbeHashMatchesAndEmpties(tophashProbe)
		if hashMatches.HasCurrent() {
			idx := hashMatches.Current()
			kv := &m.allKvs[kvsOffset+uint32(idx)]
			if kv.key == k {
				return kv.value, true
			}
		}
	}

	var zerov V
	return zerov, false
}

func MakePerfectWithUnsafeHasher[K comparable, V any](hasher func(key unsafe.Pointer, seed uintptr) uintptr, kvs []kv[K, V]) *PhFishTable[K, V] {
	if len(kvs) == 0 {
		return new(PhFishTable[K, V])
	}

	builder := new(phBuilder[K, V])
	builder.kvs = kvs
	builder.fullHashes = make([]uintptr, len(kvs))
	builder.hasher = hasher
newSeed:
	for {
		builder.seed = uintptr(runtime_fastrand64())

		{
			const maxEntriesPerMap = bucketSize
			numMaps := len(kvs) / maxEntriesPerMap
			numMaps = 1 << bits.Len(uint(numMaps))

			initialDepth := uint8(bits.TrailingZeros(uint(numMaps)))

			builder.trie = make([]*phBuilderSmolMap[K, V], numMaps)

			// TODO: WHERE'S MY __REAL__ BULK ALLOCATION?
			bulk := make([]phBuilderSmolMap[K, V], numMaps)
			for i := range builder.trie {
				builder.trie[i] = &bulk[i]
				builder.trie[i].depth = initialDepth
				builder.trie[i].hashrot = 56 // initially use the top 8 bits
			}
		}

		imNotGivingUpOnYou := builder.PutAll(kvs)
		if !imNotGivingUpOnYou {
			goto newSeed
		}

		// Optimization: because we pre-allocated the trie that may have
		// created some smolmaps that are not actually used or are so
		// under-utilized that they could be merged with their sibling map.
		//
		// This doesn't really make the lookups faster but in the best case
		// reduces metadata size by like ~5%. Worth it? No, I don't think så.
		builder.compactTheMaps()

		m := new(PhFishTable[K, V])
		m.hasher = builder.hasher
		m.seed = builder.seed

		var numTotalBuckets int
		builder.iterateMapsWithRevisiting(func(i, firstInstance int, sm *phBuilderSmolMap[K, V]) {
			add := 1
			if i != firstInstance {
				add = 0
			}
			numTotalBuckets += add
		})

		// In the final trie we pack bucket offset to a 26-bit integer.
		if numTotalBuckets >= 1<<26 {
			// On average one bucket seems to contain >8 kv pairs so we get
			// to store 2^26 * 2^3 = 2^29 kv pairs with a breddy gud
			// likelihood.
			//
			// ~512 million kvs should be enough for everyone! t. Will
			// Rates.
			panic("too many kv pairs, sorry :-|")
		}

		m.trie = make([]phtrieEntry, len(builder.trie))
		m.allMetas = make([]phbucket, numTotalBuckets)
		m.allKvs = make([]kv[K, V], len(kvs))

		var bucketsOff int
		var kvsOff int
		builder.iterateMapsWithRevisiting(func(i, firstInstance int, sm *phBuilderSmolMap[K, V]) {
			if i != firstInstance {
				// we are supposed to point to the same thing as the first instance
				m.trie[i] = m.trie[firstInstance]
				return
			}

			if sm.hashrot >= 64 {
				panic("oops")
			}

			m.trie[i] = phtrieEntry{
				_bucketsOffset_hashrot: uint32(bucketsOff)<<6 | uint32(sm.hashrot),
			}

			dstMetas := &m.allMetas[bucketsOff]
			dstMetas.kvsOffset = uint32(kvsOff)

			finder := sm.bucketmetas.Finder()
			presents := finder.PresentSlots()
			var nextSlot int
			for ; presents.HasCurrent(); presents.Advance() {
				slotInBucket := presents.Current()
				kvIndex := sm.buckets[slotInBucket]

				m.allKvs[kvsOff] = builder.kvs[kvIndex]
				kvsOff++

				// Pack the tophashes compactly to the start. They could be
				// scattered around the array if there has been any splitting
				// going on. We must do this so that the hash always matches in
				// an index that is in [0, sm.num_populated_entries)
				//
				// TODO: I'm pretty sure that there's a SIMD instruction for
				// this and this could be done outside the loop.
				dstMetas.tophash8[nextSlot] = sm.bucketmetas.tophash8[slotInBucket]
				nextSlot++
			}

			bucketsOff += 1
		})

		return m
	}
}

type phBuilder[K comparable, V any] struct {
	kvs        []kv[K, V]
	fullHashes []uintptr

	hasher func(key unsafe.Pointer, seed uintptr) uintptr
	trie   []*phBuilderSmolMap[K, V] // TODO: Maybe store (smolMap.depth) here to get better use of the cache-lines
	seed   uintptr

	// Real languages have bulk free() so freeing all of the the little maps
	// should be fast.
}

type phBuilderSmolMap[K comparable, V any] struct {
	// NOTE: len(bucketmetas) is not always a power-of-two
	bucketmetas phbucketmeta
	// NOTE: len(buckets) is not always a power-of-two
	buckets [bucketSize]int

	depth   uint8 // <= 64
	hashrot uint8 // <= 56
}

func (h *phBuilder[K, V]) compactTheMaps() {
	h.iterateMapsWithRevisiting(func(i, f int, sm *phBuilderSmolMap[K, V]) {
		if i != f {
			// already visited this
			return
		}

		// Merging will continue as long as morale improves
		for h.trie[i].depth > 0 {
			hibi := uintptr(1) << uintptr(sm.depth-1)
			step := hibi
			siblingPos := uintptr(i) & (hibi - 1)
			firstMap := h.trie[siblingPos]
			secondMap := h.trie[siblingPos+step]

			if firstMap.depth != secondMap.depth {
				return
			}

			firstFinder := firstMap.bucketmetas.Finder()
			secondFinder := secondMap.bucketmetas.Finder()

			firstPresents := firstFinder.PresentSlots()
			secondPresents := secondFinder.PresentSlots()
			if totalPop := firstPresents.Count() + secondPresents.Count(); totalPop > bucketSize {
				// No room for merging
				return
			}

			smallerMap, biggerMap := firstMap, secondMap
			if secondPresents.Count() < firstPresents.Count() {
				smallerMap, biggerMap = secondMap, firstMap
			}

			finder := smallerMap.bucketmetas.Finder()
			presents := finder.PresentSlots()

			mergedMap := *biggerMap

		insertloop:
			for ; presents.HasCurrent(); presents.Advance() {
				slotInBucket := presents.Current()
				kvIndex := smallerMap.buckets[slotInBucket]
				hash := h.fullHashes[kvIndex]

				tophash8 := fixTophash(uint8(bits.RotateLeft(uint(hash), -int(mergedMap.hashrot))))
				tophashProbe, _ := makeHashProbes(tophash8, 0)

				mergedFinder := mergedMap.bucketmetas.Finder()
				matches, empties := mergedFinder.ProbeHashMatchesAndEmpties(tophashProbe)
				if !matches.HasCurrent() {
					if !empties.HasCurrent() {
						panic("oops")
					}

					idx := empties.Current()
					mergedMap.buckets[idx] = kvIndex
					mergedMap.bucketmetas.tophash8[idx] = tophash8
					continue insertloop
				}

				newHashrot := mergedMap.hashrot
				for i := 0; i < 8; i++ {
					newHashrot -= 4
					if newHashrot < 24 {
						// don't delve too deep into the triehash bits
						newHashrot = 56
					}
					if ok := h.rehashAndAdd(&mergedMap, newHashrot, hash, kvIndex); ok {
						continue insertloop
					}
				}

				// above loop failed to merge :-(
				return
			}

			*firstMap = mergedMap
			firstMap.depth--

			// Update the trie
			for i := siblingPos; i < uintptr(len(h.trie)); i += step {
				h.trie[i] = firstMap
			}
		}
	})

	// try to reduce the trie size
reduceloop:
	for len(h.trie) > 1 {
		firstHalf, secondHalf := h.trie[:len(h.trie)/2], h.trie[len(h.trie)/2:]
		for i := range firstHalf {
			if firstHalf[i] != secondHalf[i] {
				break reduceloop
			}
		}
		h.trie = firstHalf
	}
}

func (h *phBuilder[K, V]) rehashAndAdd(m *phBuilderSmolMap[K, V], newrot uint8, frenHash uintptr, frenKvIndex int) bool {
	tophash8 := fixTophash(uint8(bits.RotateLeft(uint(frenHash), -int(newrot))))

	var dstmetas phbucketmeta
	var dstbuckets [bucketSize]int
	dstmetas.tophash8[0] = tophash8
	dstbuckets[0] = frenKvIndex
	nextFreeInDst := 1

	bucketmetas := &m.bucketmetas
	bucket := &m.buckets

	finder := bucketmetas.Finder()

	presents := finder.PresentSlots()

	for ; presents.HasCurrent(); presents.Advance() {
		slotInBucket := presents.Current()
		hash := h.fullHashes[bucket[slotInBucket]]
		tophash8 := fixTophash(uint8(bits.RotateLeft(uint(hash), -int(newrot))))
		tophash8Probe, _ := makeHashProbes(tophash8, 0)

		finder := dstmetas.Finder()
		matches, _ := finder.ProbeHashMatchesAndEmpties(tophash8Probe)
		if matches.HasCurrent() {
			return false
		}

		dstmetas.tophash8[nextFreeInDst] = tophash8
		dstbuckets[nextFreeInDst] = bucket[slotInBucket]
		nextFreeInDst++
	}

	m.bucketmetas = dstmetas
	m.buckets = dstbuckets
	m.hashrot = newrot

	return true
}

func (m *phBuilder[K, V]) PutAll(kvs []kv[K, V]) bool {
kvsLoop:
	for kvIndex := range kvs {
		k := &kvs[kvIndex].key

		hash := m.hasher(noescape(unsafe.Pointer(k)), m.seed)
		m.fullHashes[kvIndex] = hash

	retryloop:
		for {
			trie := m.trie
			trieSlot := hash & uintptr(len(trie)-1)
			if trieSlot >= uintptr(len(trie)) {
				panic("oops")
			}

			sm := trie[trieSlot]

			mapmetas := &sm.bucketmetas
			mapbuckets := &sm.buckets

			tophash8 := fixTophash(uint8(bits.RotateLeft(uint(hash), -int(sm.hashrot))))

			tophashProbe, _ := makeHashProbes(tophash8, 0)

			bucketmetas := mapmetas
			bucket := mapbuckets

			finder := bucketmetas.Finder()
			hashMatches, empties := finder.ProbeHashMatchesAndEmpties(tophashProbe)
			if hashMatches.HasCurrent() {
				// oh nyoooo... A tophash collision! Perhaps even a full hash
				// collision! Test it.
				idx := hashMatches.Current()
				if m.fullHashes[bucket[idx]] == hash {
					if m.kvs[bucket[idx]].key == *k {
						panic("TODO?")
					}
					// Two keys share the same hash... Shit!
					return false
				}

				if empties.HasCurrent() {
					// WE WILL NOT TOLERATE SPLITTING IF WE HAVE EMPTY SLOTS
					//
					// ... also, I would really like to have my 128-bit hash
					// values right now.
					newHashrot := sm.hashrot
					for i := 0; i < 8; i++ {
						newHashrot -= 4
						if newHashrot < 24 {
							// don't delve too deep into the triehash bits
							newHashrot = 56
						}
						if ok := m.rehashAndAdd(sm, newHashrot, hash, kvIndex); ok {
							continue kvsLoop
						}
					}
				}

				// Just a top-hash collision. SPLIT and try again
				m.splitForHash(sm, hash)
				continue retryloop
			}

			if empties.HasCurrent() {
				idx := empties.Current()
				bucket[idx] = kvIndex
				bucketmetas.tophash8[idx] = tophash8
				continue kvsLoop
			}

			// No empty space? SPLIT and try again!
			m.splitForHash(sm, hash)
		}
	}
	return true
}

func (m *phBuilder[K, V]) splitForHash(sm *phBuilderSmolMap[K, V], hash uintptr) {
	// split it
	left, right := m.split(sm)

	if left != sm {
		// the trie update loop below requires this
		panic("oops")
	}

	// But maybe the trie needs to grow as well...
	oldDepth := left.depth - 1
	if 1<<oldDepth == len(m.trie) {
		oldTrie := m.trie
		m.trie = make([]*phBuilderSmolMap[K, V], len(oldTrie)*2)
		copy(m.trie[:len(oldTrie)], oldTrie)
		copy(m.trie[len(oldTrie):], oldTrie)
	}

	hibi := uintptr(1) << uintptr(oldDepth)
	step := hibi
	for i := uintptr(hash&(hibi-1)) + step; i < uintptr(len(m.trie)); i += (step * 2) {
		m.trie[i] = right
	}
}

func (h *phBuilder[K, V]) split(m *phBuilderSmolMap[K, V]) (*phBuilderSmolMap[K, V], *phBuilderSmolMap[K, V]) {
	if m.depth == 64 {
		// 64-bit hash values have only 64 bits :-(
		panic("depth overflow")
	}

	oldDepthBit := bits.RotateLeft64(1, int(m.depth))

	m.depth++

	// Re-use m as left
	left := m

	right := new(phBuilderSmolMap[K, V])
	right.depth = m.depth
	right.hashrot = m.hashrot

	var nextFreeInRight uint8

	bucketmetas := &m.bucketmetas
	bucket := &m.buckets

	finder := bucketmetas.Finder()

	presents := finder.PresentSlots()

	// Move righties to the right map
	for ; presents.HasCurrent(); presents.Advance() {
		slotInBucket := presents.Current()
		hash := h.fullHashes[bucket[slotInBucket]]
		if goesRight := hash & uintptr(oldDepthBit); goesRight == 0 {
			continue
		}

		dstmetas := &right.bucketmetas
		dstbuckets := &right.buckets
		dstmetas.tophash8[nextFreeInRight] = bucketmetas.tophash8[slotInBucket]
		dstbuckets[nextFreeInRight] = bucket[slotInBucket]
		nextFreeInRight++

		// Zero the slot left behind. Only setting the hashes is strictly necessary
		bucketmetas.tophash8[slotInBucket] = 0
		bucket[slotInBucket] = 0
	}

	return left, right
}

func (m *phBuilder[K, V]) iterateMapsWithRevisiting(iter func(int, int, *phBuilderSmolMap[K, V])) {
	// Oh... god... modifying iterators... pain...
	for i, sm := range m.trie {
		triehash := i

		// Did we visit this map already? Eerily similar to the logic elsewhere ;)
		hibi := bits.RotateLeft64(1, int(sm.depth))
		firstInstance := int((uint64(triehash) & (hibi - 1)))

		iter(i, firstInstance, sm)
	}
}
