//go:build amd64 && !nosimd

package finnishtable

import (
	"math/bits"

	"github.com/rip-create-your-account/finnishtable/asm"
)

const bucketSize = 16

type (
	tophashprobe  uint8
	triehashprobe uint8
)

func makeHashProbes(tophash8 tophash, triehash8 uint8) (tophashprobe, triehashprobe) {
	// Imagine building the 128-bit XMM register here.
	//     MOVD     tophash8, X_tophash8
	//     PSHUFB   X_ZERO, X_tophash8
	return tophashprobe(tophash8), triehashprobe(triehash8)
}

type bucketfinder struct{ *bucketmeta }

func (b *bucketmeta) Finder() bucketfinder {
	return bucketfinder{b}
}

func (b *bucketfinder) ProbeHashMatchesAndEmpties(tophashProbe tophashprobe, triehashProbe triehashprobe) (hashes, empties matchiter) {
	hashes.hashMatches, empties.hashMatches = asm.FindHashesAndEmpties(&b.tophash8, &b.triehash8, uint8(tophashProbe), uint8(triehashProbe))
	return
}

func (b *bucketfinder) EmptySlots() matchiter {
	hashMatches := asm.FindEmpty(&b.tophash8)
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) PresentSlots() matchiter {
	hashMatches := asm.FindBytesWhereBitsRemainSetAfterApplyingThisMask(&b.tophash8, 0b0111_1111)
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) MakeTombstonesIntoEmpties(m *bucketmeta) {
	matches := findBytesInKindaSlowWay(&b.tophash8, tophashTombstone)
	for ; matches.HasCurrent(); matches.Advance() {
		b.tophash8[matches.Current()] = tophashEmpty
	}
}

func (b *bucketfinder) GoesRightForBit(maskWithBitSet uint8) matchiter {
	// We can find all entries that would go to the "right" for the given bit
	// by also noticing that only for present (not empty or tombstone) entries
	// the triehash bits can be non-zero. So we just find the triehash entries
	// where the given bit is 1 and we know that entries there are also
	// present.
	hashMatches := asm.FindBytesWhereBitsRemainSetAfterApplyingThisMask(&b.triehash8, maskWithBitSet)
	return matchiter{hashMatches: hashMatches}
}

type matchiter struct{ hashMatches uint16 }

func (m *matchiter) HasCurrent() bool {
	return m.hashMatches != 0
}

func (m *matchiter) Current() uint8 {
	// TODO: Remove masking when the compiler is smarter
	return uint8(bits.TrailingZeros16(m.hashMatches)) & (16 - 1)
}

func (m *matchiter) Advance() {
	// unset the lowest set bit
	// BLSR — Reset Lowest Set Bit - works only on 32 or 64 bit integers (and
	// requires compiling with GOAMD64=v3)
	m.hashMatches = m.hashMatches & (m.hashMatches - 1)
}

func (m *matchiter) Count() uint8 {
	return uint8(bits.OnesCount16(m.hashMatches))
}

func (m *matchiter) AsBitmask() bucketBitmask {
	return bucketBitmask(m.hashMatches)
}

func (m *matchiter) Invert() matchiter {
	return matchiter{hashMatches: ^uint16(m.hashMatches)}
}

func (m *matchiter) WithBitmaskExcluded(bm bucketBitmask) matchiter {
	return matchiter{hashMatches: m.hashMatches &^ uint16(bm)}
}

func findBytesInKindaSlowWay(bytes *[bucketSize]uint8, b uint8) matchiter {
	// NOTE: We could also just create a copy of bytes and with SWAR xor
	// matching bytes into zeros and pass that to FindEmpty()
	// TODO: Alignment of "bytes" is spooky? Does it matter?
	var fakeTriehashes [16]uint8 // fake triehashes that always match with '0'
	hashMatches, _ := asm.FindHashesAndEmpties(bytes, &fakeTriehashes, b, 0)
	return matchiter{hashMatches: hashMatches}
}

type bucketBitmask uint16

func (bm bucketBitmask) IsMarked(slot uint8) bool {
	return (bm>>slot)&1 == 1
}

func (bm *bucketBitmask) Mark(slot uint8) {
	*bm |= 1 << slot
}

func (bm *bucketBitmask) Unmark(slot uint8) {
	*bm &^= 1 << slot
}

func (bm bucketBitmask) FirstUnmarkedSlot() uint8 {
	return uint8(bits.TrailingZeros16(^uint16(bm)))
}

func (bm bucketBitmask) AsMatchiter() matchiter {
	return matchiter{hashMatches: uint16(bm)}
}

type phbucketfinder struct{ tophash8 *[bucketSize]byte }

func phbucketFinderFrom(tophash8 *[bucketSize]byte) phbucketfinder {
	return phbucketfinder{tophash8}
}

var zeroes16 [16]uint8

func (b *phbucketfinder) ProbeHashMatches(tophashProbe tophashprobe) (hashes matchiter) {
	hashes.hashMatches, _ = asm.FindHashesAndEmpties(b.tophash8, &zeroes16, uint8(tophashProbe), 0)
	return
}
