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
	return tophashprobe(tophash8), triehashprobe(triehash8)
}

type bucketfinder struct{ *bucketmeta }

func (b *bucketmeta) Finder() bucketfinder {
	return bucketfinder{b}
}

func (b *bucketfinder) ProbeHashMatches(tophashProbe tophashprobe, triehashProbe triehashprobe) matchiter {
	hashMatches := asm.FindHashes(&b.tophash8, &b.triehash8, uint8(tophashProbe), uint8(triehashProbe))
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) EmptySlots() matchiter {
	hashMatches := asm.FindEmpty(&b.tophash8)
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) PresentSlots() matchiter {
	hashMatches := asm.FindPresent(&b.tophash8)
	return matchiter{hashMatches: hashMatches}
}

func (b *bucketfinder) MakeTombstonesIntoEmpties(m *bucketmeta) {
	matches := findBytesInKindaSlowWay(&b.tophash8, tophashTombstone)
	for ; matches.HasCurrent(); matches.Advance() {
		b.tophash8[matches.Current()] = tophashEmpty
	}
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
	h := uint32(m.hashMatches)
	m.hashMatches = uint16(h & (h - 1))
}

func (m *matchiter) Count() uint8 {
	return uint8(bits.OnesCount16(m.hashMatches))
}

func findZeros(bytes *[bucketSize]uint8) matchiter {
	// TODO: Alignment of "bytes" is spooky? Does it matter?
	hashMatches := asm.FindEmpty(bytes)
	return matchiter{hashMatches: hashMatches}
}

func findBytesInKindaSlowWay(bytes *[bucketSize]uint8, b uint8) matchiter {
	// NOTE: We could also just create a copy of bytes and with SWAR xor
	// matching bytes into zeros and pass that to FindEmpty()
	// TODO: Alignment of "bytes" is spooky? Does it matter?
	var fakeTriehashes [16]uint8 // fake triehashes that always match with '0'
	hashMatches := asm.FindHashes(bytes, &fakeTriehashes, b, 0)
	return matchiter{hashMatches: hashMatches}
}
