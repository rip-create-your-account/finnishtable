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
		TEXT("FindHashes", NOSPLIT, "func(tophashes, triehashes *[16]uint8, tophash8, triehash8 uint8) uint16")
		Pragma("noescape") // tophashes and triehashes have no need to escape

		b1 := Mem{Base: Load(Param("tophashes"), GP64())}
		b2 := Mem{Base: Load(Param("triehashes"), GP64())}
		h1 := Load(Param("tophash8"), GP32())
		h2 := Load(Param("triehash8"), GP32())
		out := GP32()

		x0, x1, x2, x3 := XMM(), XMM(), XMM(), XMM()

		MOVD(h1, x0)
		MOVD(h2, x2)

		PXOR(x1, x1)
		PSHUFB(x1, x0)
		PSHUFB(x1, x2)

		MOVOU(b1, x1)
		MOVOU(b2, x3)

		PCMPEQB(x1, x0)
		PCMPEQB(x3, x2)
		PAND(x2, x0)

		PMOVMSKB(x0, out)

		Store(out.As16(), ReturnIndex(0))
		RET()
	}

	{
		TEXT("FindEmpty", NOSPLIT, "func(tophashes *[16]uint8) uint16")
		Pragma("noescape") // tophashes has no need to escape

		b1 := Mem{Base: Load(Param("tophashes"), GP64())}
		out := GP32()

		x0, x1 := XMM(), XMM()
		MOVOU(b1, x1)

		PXOR(x0, x0)
		PCMPEQB(x1, x0)

		PMOVMSKB(x0, out)

		Store(out.As16(), ReturnIndex(0))
		RET()
	}

	{
		// To figure out the slots with a present value (not empty or
		// tombstone), we remember that only for empty and tombstone marker
		// tophashes the lowest 7 bits are all zeros.
		//
		// So we mask out the 8th bit and see if the remaining byte is >0.
		// TODO: Is there a better way?
		TEXT("FindPresent", NOSPLIT, "func(tophashes *[16]uint8) uint16")
		Pragma("noescape") // tophashes has no need to escape

		b1 := Mem{Base: Load(Param("tophashes"), GP64())}
		out := GP32()
		mask := GP64()

		x0, x1 := XMM(), XMM()

		MOVQ(U64(0b0111_1111), mask)
		MOVQ(mask, x0)

		PXOR(x1, x1)
		PSHUFB(x1, x0)

		MOVOU(b1, x1)

		// setup done
		PAND(x1, x0)

		PXOR(x1, x1)
		PCMPGTB(x1, x0)

		PMOVMSKB(x0, out)

		Store(out.As16(), ReturnIndex(0))
		RET()
	}

	Generate()
}
