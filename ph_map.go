package finnishtable

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"unsafe"
)

// TODO: Specialize assembly for the needs for this data structure. That would
// require modifying my lovely assembly routines so I won't bother for now. I
// prefer waiting the 10+ years until Go has SIMD intrinsics.

func fixTophashForPh(hash uint8) tophash {
	// We have no reserved values
	return hash
}

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
	// So... bits.RotateLeft/shifts only looks at the lower 6 bits. So
	// basically I'm just gonna not mask off the upper bits. It's kinda nice
	// that bucketsOffset is just a shr instruction and this one is nothing.
	return e._bucketsOffset_hashrot
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
	// eg. {HDR0, tb0, tb1, tb2, tb3, tb4, kv0, kv1, kv2, kv3, kv4, HDR1, tb0,
	// tb1, tb2, kv0, kv1, kv2, HDR2, tb0, tb1, ...}
	//
	// Less cache misses all around, l1, l2 ... tlb, etc...
	allKvs   []kv[K, V]
	allMetas []uint8 // "encoded" (bucket header + bucket tophashes)s. See Get() for how to decode.

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

		metastart := (*[4 + bucketSize]byte)(m.allMetas[sm.bucketsOffset():])

		hdr := binary.LittleEndian.Uint32(metastart[0:])
		kvsOffset := hdr
		bucketmetas := (*[bucketSize]byte)(metastart[4 : 4+bucketSize])

		tophash8 := fixTophashForPh(uint8(hash >> (sm.hashrot() % 64)))
		tophashProbe, _ := makeHashProbes(tophash8, 0)

		// NOTE: The nice thing about having to find the matching tophash is
		// the case when there's none - because the key is not in the map! So
		// with a good likelihood we don't have to waste our time comparing the
		// keys. Goddamn is this shit practical and nice and stuff.
		finder := phbucketFinderFrom(bucketmetas)
		hashMatches := finder.ProbeHashMatches(tophashProbe)
		if hashMatches.HasCurrent() {
			// NOTE: We are potentially probing to the next bucket, but here we
			// take the earliest match so there's no room for failure. Well...
			// There's room for failure if we didn't also check the key
			// equality, but we do. If we don't want to check for the key then
			// we might have to include the size of the bucket in the bucket
			// metadata so that we know not to match beyond that.
			slotInBucket := hashMatches.Current()
			idx := uint(kvsOffset) + uint(slotInBucket)
			if idx < uint(len(m.allKvs)) { // rare branch miss only for last bucket and if tophash8(k) == 0
				kv := &m.allKvs[idx]
				if kv.key == k {
					return kv.value, true
				}
			}
		}
	}

	var zerov V
	return zerov, false
}

// Returns the unique integer for this key in [0, len(kvs)). Returns -1 if k is
// not in the set
func (m *PhFishTable[K, V]) GetInt(k K) int {
	trie := m.trie
	if len(trie) > 0 {
		hash := m.hasher(noescape(unsafe.Pointer(&k)), m.seed)

		sm := &trie[hash&uintptr(len(trie)-1)]

		metastart := (*[4 + bucketSize]byte)(m.allMetas[sm.bucketsOffset():])

		hdr := binary.LittleEndian.Uint32(metastart[0:])
		kvsOffset := hdr
		bucketmetas := (*[bucketSize]byte)(metastart[4 : 4+bucketSize])

		tophash8 := fixTophashForPh(uint8(hash >> (sm.hashrot() % 64)))
		tophashProbe, _ := makeHashProbes(tophash8, 0)

		// NOTE: The nice thing about having to find the matching tophash is
		// the case when there's none - because the key is not in the map! So
		// with a good likelihood we don't have to waste our time comparing the
		// keys. Goddamn is this shit practical and nice and stuff.
		finder := phbucketFinderFrom(bucketmetas)
		hashMatches := finder.ProbeHashMatches(tophashProbe)
		if hashMatches.HasCurrent() {
			// NOTE: We are potentially probing to the next bucket, but here we
			// take the earliest match so there's no room for failure. Well...
			// There's room for failure if we didn't also check the key
			// equality, but we do. If we don't want to check for the key then
			// we might have to include the size of the bucket in the bucket
			// metadata so that we know not to match beyond that.
			slotInBucket := hashMatches.Current()
			idx := uint(kvsOffset) + uint(slotInBucket)
			if idx < uint(len(m.allKvs)) { // rare branch miss only for last bucket and if tophash8(k) == 0
				kv := &m.allKvs[idx]
				if kv.key == k {
					return int(idx)
				}
			}
		}
	}

	return -1
}

func MakePerfectWithUnsafeHasher[K comparable, V any](hasher func(key unsafe.Pointer, seed uintptr) uintptr, kvs []kv[K, V]) *PhFishTable[K, V] {
	if len(kvs) == 0 {
		return new(PhFishTable[K, V])
	}
	if len(kvs) > math.MaxUint32 {
		panic("too many keys")
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
			adjustedSize := len(kvs) * 16 / 11 // 11/16 is the load-factor that we hit consistently
			numMaps := adjustedSize / maxEntriesPerMap
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
		// The main benefit would come from it reducing the size of the trie
		// but that's very unlikely to happen.
		builder.compactTheMaps()

		m := new(PhFishTable[K, V])
		m.hasher = builder.hasher
		m.seed = builder.seed

		var numTotalBuckets int
		var lastBucketPop int
		builder.iterateMapsWithRevisiting(func(i, firstInstance int, sm *phBuilderSmolMap[K, V]) {
			if i != firstInstance {
				return
			}

			numTotalBuckets++

			lastBucketPop = int(sm.pop)
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

		// Calculate size for allmetas and ensure that we can do a 16-byte read
		// even for the last bucket
		encodedSize := numTotalBuckets*4 + len(kvs)*1
		encodedSize += bucketSize - lastBucketPop

		m.trie = make([]phtrieEntry, len(builder.trie))
		m.allMetas = make([]uint8, encodedSize)
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

			if kvsOff >= 1<<27 {
				panic("oops")
			}

			dstMetas := m.allMetas[bucketsOff:]

			// first comes the 32-bits of header data
			binary.LittleEndian.PutUint32(dstMetas, uint32(kvsOff))

			// then the tophashes
			dstHashes := dstMetas[4:][:sm.pop]
			for slotInBucket := range sm.buckets[:sm.pop] {
				kvIndex := sm.buckets[slotInBucket]

				hash := builder.fullHashes[kvIndex]
				tophash8 := fixTophashForPh(uint8(hash >> (sm.hashrot % 64)))
				dstHashes[slotInBucket] = tophash8

				m.allKvs[kvsOff] = builder.kvs[kvIndex]
				kvsOff++
			}
			bucketsOff += 4 + int(sm.pop)
		})

		return m
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

type phBuilder[K comparable, V any] struct {
	kvs        []kv[K, V]
	fullHashes []uintptr

	hasher func(key unsafe.Pointer, seed uintptr) uintptr
	trie   []*phBuilderSmolMap[K, V] // TODO: Maybe store (smolMap.depth) here to get better use of the cache-lines
	seed   uintptr

	// Real languages have bulk free() so freeing all of the the little maps
	// should be fast.
}

type bitvector [4]uint64

func (bv *bitvector) Toggle(i uint8) {
	bit := uint64(1) << (i % 64)
	bv[i>>6] ^= bit
}

func (bv *bitvector) IsSet(i uint8) bool {
	bit := uint64(1) << (i % 64)
	return bv[i>>6]&bit != 0
}

type phBuilderSmolMap[K comparable, V any] struct {
	buckets [bucketSize]uint32
	// keeps track of the present tophashes in [0, 255]
	//
	// NOTE: With proper SIMD you probably just want to store the 16 tophashes
	// with pop count. It would use less space and probably would be faster.
	// Especially considering that we wouldn't have to do work to find the
	// intruder when we have a tophash collision.
	bitvector bitvector
	pop       uint8 // <= 16

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

			if totalPop := firstMap.pop + secondMap.pop; totalPop > bucketSize {
				// No room for merging
				return
			}

			smallerMap, biggerMap := firstMap, secondMap
			if secondMap.pop < firstMap.pop {
				smallerMap, biggerMap = secondMap, firstMap
			}

			// create a copy that we can safely mutate
			mergedMap := *biggerMap

			// TODO: With proper SIMD there's likely a better algorithm for
			// this. Real 256-bit bitvector would make certain things easier.
		insertloop:
			for slotInBucket := range smallerMap.buckets[:smallerMap.pop] {
				kvIndex := smallerMap.buckets[slotInBucket]
				hash := h.fullHashes[kvIndex]

				tophash8 := fixTophashForPh(uint8(hash >> (mergedMap.hashrot % 64)))
				if !mergedMap.bitvector.IsSet(tophash8) {
					if mergedMap.pop >= bucketSize {
						panic("oops")
					}

					idx := mergedMap.pop
					mergedMap.buckets[idx] = kvIndex
					mergedMap.bitvector.Toggle(tophash8)
					mergedMap.pop++
					continue insertloop
				}

				if ok := h.findWorkingHashrotAndAdd(&mergedMap, hash, kvIndex); ok {
					continue insertloop
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

func (h *phBuilder[K, V]) findWorkingHashrotAndAdd(m *phBuilderSmolMap[K, V], frenHash uintptr, frenKvIndex uint32) bool {
	newrot := m.hashrot
nextrot:
	for i := 0; i < 8; i++ {
		newrot -= 4
		if newrot <= m.depth {
			// Don't delve too deep into the triehash bits as they are equal
			// for all entries in the smol map
			newrot = 56
		}

		var bv bitvector

		// Try to re-slot the original values with the new hashrot
		for slotInBucket := range m.buckets[:m.pop] {
			hash := h.fullHashes[m.buckets[slotInBucket]]
			tophash8 := fixTophashForPh(uint8(hash >> (newrot % 64)))

			// NOTE: One would assume that here the compiler would use the BTS
			// instruction and check the carry flag to see the previous value
			// for the bit. But apparently all compilers shit on the
			// (BTS+carry) combo. How come? I want to see some of that BTS+JC
			// action.
			if bv.IsSet(tophash8) {
				continue nextrot
			}
			bv.Toggle(tophash8)
		}

		// Then try to add the new one
		{
			tophash8 := fixTophashForPh(uint8(frenHash >> (newrot % 64)))
			if bv.IsSet(tophash8) {
				continue nextrot
			}

			// take the first free slot for it
			freeSlotForInsert := m.pop
			bv.Toggle(tophash8)
			m.buckets[freeSlotForInsert] = frenKvIndex
			m.pop++
		}

		m.bitvector = bv
		m.hashrot = newrot
		return true
	}

	return false
}

func (m *phBuilder[K, V]) PutAll(kvs []kv[K, V]) bool {
kvsLoop:
	for _kvIndex := range kvs {
		kvIndex := uint32(_kvIndex)
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

			tophash8 := fixTophashForPh(uint8(hash >> (sm.hashrot % 64)))

			if sm.bitvector.IsSet(tophash8) {
				// oh nyoooo... A tophash collision! Perhaps even a full hash
				// collision! Test it.

				// Loop through all entries to find the intruder
				for slotInBucket := range sm.buckets[:sm.pop] {
					otherhash := m.fullHashes[sm.buckets[slotInBucket]]
					if otherhash == hash {
						if m.kvs[sm.buckets[slotInBucket]].key == *k {
							panic("TODO?")
						}
						// Two keys share the same hash... Shit!
						return false
					}
				}

				if sm.pop < bucketSize {
					// WE WILL NOT TOLERATE SPLITTING IF WE HAVE EMPTY SLOTS
					//
					// ... also, I would really like to have my 128-bit hash
					// values right now.

					if ok := m.findWorkingHashrotAndAdd(sm, hash, kvIndex); ok {
						continue kvsLoop
					}
				}

				// Just a top-hash collision. SPLIT and try again
				m.splitForHash(sm, hash)
				continue retryloop
			}

			if sm.pop < bucketSize {
				idx := sm.pop
				sm.buckets[idx] = kvIndex
				sm.bitvector.Toggle(tophash8)
				sm.pop++
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

	oldDepthBit := uint64(1) << (m.depth % 64)

	m.depth++

	// Re-use m as left
	left := m

	right := new(phBuilderSmolMap[K, V])
	right.depth = m.depth
	right.hashrot = m.hashrot

	// Move righties to the right map
	for slotInBucket := range left.buckets[:left.pop] {
		hash := h.fullHashes[left.buckets[slotInBucket]]
		tophash8 := fixTophashForPh(uint8(hash >> (left.hashrot % 64)))
		if goesRight := hash & uintptr(oldDepthBit); goesRight == 0 {
			// we want to keep our entries at the head of the array, so as we
			// remove righties we leave gaps, here we fill them
			moveto := slotInBucket - int(right.pop)
			left.buckets[moveto] = left.buckets[slotInBucket]
			continue
		}

		nextFreeInRight := right.pop
		right.buckets[nextFreeInRight] = left.buckets[slotInBucket]
		right.pop++
		right.bitvector.Toggle(tophash8)

		// Zero the slot left behind. Only setting the hashes is strictly necessary
		left.bitvector.Toggle(tophash8)
	}

	left.pop -= right.pop

	return left, right
}

func (m *phBuilder[K, V]) iterateMapsWithRevisiting(iter func(int, int, *phBuilderSmolMap[K, V])) {
	// Oh... god... modifying iterators... pain...
	for i, sm := range m.trie {
		triehash := i

		// Did we visit this map already? Eerily similar to the logic elsewhere ;)
		hibi := uint64(1) << (sm.depth % 64)
		firstInstance := int((uint64(triehash) & (hibi - 1)))

		iter(i, firstInstance, sm)
	}
}
