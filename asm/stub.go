// Code generated by command: go run generate.go -out add.s -stubs stub.go. DO NOT EDIT.

//go:build amd64

package asm

//go:noescape
func FindHashes(tophashes *[16]uint8, triehashes *[16]uint8, tophash8 uint8, triehash8 uint8) uint16

//go:noescape
func FindEmpty(tophashes *[16]uint8) uint16

//go:noescape
func FindPresent(tophashes *[16]uint8) uint16
