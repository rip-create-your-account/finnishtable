//go:build ignore

package main

//go:generate go run generate.go -out add.s -stubs stub.go

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	// TODO: arm64
	ConstraintExpr("amd64")

	{
		TEXT("FindHashesAndEmpties", NOSPLIT, "func(tophashes, triehashes *[16]uint8, tophash8, triehash8 uint8) (hashes, empties uint16)")
		Pragma("noescape") // tophashes and triehashes have no need to escape

		b1 := Mem{Base: Load(Param("tophashes"), GP64())}
		b2 := Mem{Base: Load(Param("triehashes"), GP64())}
		h1 := Load(Param("tophash8"), GP32())
		h2 := Load(Param("triehash8"), GP32())
		hashmatches := GP32()
		empties := GP32()

		xtophash, xtriehash, xtophashes, xtriehashes, xzero := XMM(), XMM(), XMM(), XMM(), XMM()

		MOVD(h1, xtophash)
		MOVD(h2, xtriehash)

		PXOR(xzero, xzero)
		PSHUFB(xzero, xtophash)
		PSHUFB(xzero, xtriehash)

		MOVOU(b1, xtophashes)
		MOVOU(b2, xtriehashes)

		PCMPEQB(xtophashes, xtophash)
		PCMPEQB(xtriehashes, xtriehash)
		PAND(xtophash, xtriehash)

		PMOVMSKB(xtriehash, hashmatches)

		Comment("Then the empties")
		PCMPEQB(xzero, xtophashes)
		PMOVMSKB(xtophashes, empties)

		Store(hashmatches.As16(), ReturnIndex(0))
		Store(empties.As16(), ReturnIndex(1))
		RET()
	}

	{
		TEXT("FindEmpty", NOSPLIT, "func(tophashes *[16]uint8) uint16")
		Pragma("noescape") // tophashes has no need to escape

		b1 := Mem{Base: Load(Param("tophashes"), GP64())}
		out := GP32()

		xhashes, xzero := XMM(), XMM()
		MOVOU(b1, xhashes)

		PXOR(xzero, xzero)
		PCMPEQB(xzero, xhashes)

		PMOVMSKB(xhashes, out)

		Store(out.As16(), ReturnIndex(0))
		RET()
	}

	{
		TEXT("FindBytesWhereBitsRemainSetAfterApplyingThisMask", NOSPLIT, "func(hashes *[16]uint8, mask uint8) uint16")
		Pragma("noescape") // hashes has no need to escape

		b1 := Mem{Base: Load(Param("hashes"), GP64())}
		mask := Load(Param("mask"), GP64())
		out := GP32()

		xmask, xhashes, xzero := XMM(), XMM(), XMM()

		MOVQ(mask, xmask)

		PXOR(xzero, xzero)
		PSHUFB(xzero, xmask)

		MOVOU(b1, xhashes)

		Comment("Figure out the bytes where all bits are ZEROES after applying the mask, then invert the result")
		PAND(xmask, xhashes)
		PCMPEQB(xzero, xhashes)
		PMOVMSKB(xhashes, out)
		NOTL(out)

		Store(out.As16(), ReturnIndex(0))
		RET()
	}

	Generate()
}
