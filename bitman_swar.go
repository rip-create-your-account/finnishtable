//go:build amd64 && nosimd

package finnishtable

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const bucketSize = 8

type (
	tophashprobe  uint8
	triehashprobe uint8
)

func makeHashProbes(tophash8 tophash, triehash8 uint8) (tophashprobe, triehashprobe) {
	findTophash := (math.MaxUint64 / 255) * uint64(tophash8)
	findTriehash := (math.MaxUint64 / 255) * uint64(triehash8)
	return tophashprobe(findTophash), triehashprobe(findTriehash)
}

type bucketfinder struct {
	tophashes8le, triehashes8le uint64
}

func (b *bucketmeta) Finder() bucketfinder {
	return bucketfinder{
		tophashes8le:  binary.LittleEndian.Uint64(b.tophash8[:]),
		triehashes8le: binary.LittleEndian.Uint64(b.triehash8[:]),
	}
}

func (b *bucketfinder) ProbeHashMatches(tophashProbe tophashprobe, triehashProbe triehashprobe) matchiter {
	// find (tophashMatches & triehashMatches)
	// TODO: Try to abuse the specific values of tophashEmpty and
	// tophashTombstone for speed just like findPresentTophash64 does
	hashMatches := findZeros64((b.tophashes8le ^ uint64(tophashProbe)) | (b.triehashes8le ^ uint64(triehashProbe)))
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) EmptySlots() matchiter {
	return matchiter{hashMatches: findZeros64(b.tophashes8le)}
}

func (b *bucketfinder) PresentSlots() matchiter {
	hashMatches := findPresentTophash64(b.tophashes8le)
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) MakeTombstonesIntoEmpties(m *bucketmeta) {
	const tombies = (math.MaxUint64 / 255) * uint64(tophashTombstone)
	iter := matchiter{hashMatches: findZeros64(b.tophashes8le ^ tombies)}

	for ; iter.HasCurrent(); iter.Advance() {
		slotInBucket := iter.Current()
		m.tophash8[slotInBucket] = tophashEmpty
	}
}

type matchiter struct {
	hashMatches uint64
}

func (m *matchiter) HasCurrent() bool {
	return m.hashMatches != 0
}

func (m *matchiter) Current() uint8 {
	bit := bits.TrailingZeros64(m.hashMatches)
	idx := bit / 8
	return uint8(idx)
}

func (m *matchiter) Advance() {
	// unset the lowest set bit
	// BLSR — Reset Lowest Set Bit - works only on 32 or 64 bit integers (and
	// requires compiling with GOAMD64=v3)
	m.hashMatches = m.hashMatches & (m.hashMatches - 1)
}

func (m *matchiter) Count() uint8 {
	return uint8(bits.OnesCount64(m.hashMatches))
}

func findZeros64(v uint64) uint64 {
	const c1 = (math.MaxUint64 / 255) * 0b0111_1111
	const topbit = (math.MaxUint64 / 255) * 0b1000_0000
	return ^((v&c1 + c1) | v) & topbit
}

func findPresentTophash64(v uint64) uint64 {
	const c1 = (math.MaxUint64 / 255) * 0b0111_1111
	const topbit = (math.MaxUint64 / 255) * 0b1000_0000
	// with the current values of tophashEmpty and tophashTombstone
	// the '(... + c1)' will only set the top bit to 1 if the
	// hash is neither of those special values
	return ((v & c1) + c1) & topbit
}

func findZeros(bytes *[bucketSize]uint8) matchiter {
	hashMatches := findZeros64(binary.LittleEndian.Uint64(bytes[:]))
	return matchiter{hashMatches: hashMatches}
}
