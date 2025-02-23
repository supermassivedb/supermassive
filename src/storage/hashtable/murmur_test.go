// BSD 3-Clause License
//
// (C) Copyright 2025, Alex Gaetano Padula & SuperMassive authors
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package hashtable

import (
	"encoding/binary"
	"testing"
)

type testCase struct {
	input    []byte
	seed     uint32
	expected uint32
}

func TestMurmurHash3(t *testing.T) {
	tests := []testCase{
		// Empty input
		{
			input:    []byte{},
			seed:     0x00000000,
			expected: 0x00000000,
		},
		// Test string with zero seed
		{
			input:    []byte("test"),
			seed:     0x00000000,
			expected: 0xba6bd213,
		},
		// Test string with non-zero seed
		{
			input:    []byte("test"),
			seed:     0x9747b28c,
			expected: 0x704b81dc,
		},
		// Hello world with zero seed
		{
			input:    []byte("Hello, world!"),
			seed:     0x00000000,
			expected: 0xc0363e43,
		},
		// Hello world with non-zero seed
		{
			input:    []byte("Hello, world!"),
			seed:     0x9747b28c,
			expected: 0x24884CBA,
		},
		// Fox string with zero seed
		{
			input:    []byte("The quick brown fox jumps over the lazy dog"),
			seed:     0x00000000,
			expected: 0x2e4ff723,
		},
		// Fox string with non-zero seed
		{
			input:    []byte("The quick brown fox jumps over the lazy dog"),
			seed:     0x9747b28c,
			expected: 0x2FA826CD,
		},
	}

	for i, tc := range tests {
		result := MurmurHash3(tc.input, tc.seed)
		if result != tc.expected {
			t.Logf("Test case %d failed:\nInput: %q\nSeed: 0x%08x\nExpected: 0x%08x\nGot: 0x%08x",
				i, tc.input, tc.seed, tc.expected, result)
		} else {
			t.Logf("Test case %d passed", i)
		}
	}
}

// TestMurmurHash3Properties tests general properties that should hold for the hash function
func TestMurmurHash3Properties(t *testing.T) {
	// Test that different seeds produce different outputs for the same input
	input := []byte("test string")
	hash1 := MurmurHash3(input, 0)
	hash2 := MurmurHash3(input, 1)
	if hash1 == hash2 {
		t.Error("Different seeds should produce different hashes")
	}

	// Test that small changes in input produce different outputs
	input1 := []byte("test string")
	input2 := []byte("test string!")
	hash1 = MurmurHash3(input1, 0)
	hash2 = MurmurHash3(input2, 0)
	if hash1 == hash2 {
		t.Error("Different inputs should produce different hashes")
	}

	// Test alignment handling
	alignedInput := make([]byte, 8)
	binary.LittleEndian.PutUint64(alignedInput, 0x0123456789ABCDEF)
	hash1 = MurmurHash3(alignedInput[:4], 0) // First 4 bytes
	hash2 = MurmurHash3(alignedInput[4:], 0) // Last 4 bytes
	if hash1 == hash2 {
		t.Error("Different aligned 4-byte blocks should produce different hashes")
	}

	// Test that length affects output
	input1 = []byte{0x01, 0x02, 0x03}
	input2 = []byte{0x01, 0x02, 0x03, 0x00}
	hash1 = MurmurHash3(input1, 0)
	hash2 = MurmurHash3(input2, 0)
	if hash1 == hash2 {
		t.Error("Inputs of different lengths should produce different hashes")
	}
}

// Benchmark the hash function with different input sizes
func BenchmarkMurmurHash3(b *testing.B) {
	// Test cases with different sizes
	benchCases := []struct {
		name string
		size int
	}{
		{"Small_4B", 4},
		{"Medium_100B", 100},
		{"Large_1KB", 1024},
		{"XLarge_10KB", 10240},
	}

	for _, bc := range benchCases {
		input := make([]byte, bc.size)
		for i := range input {
			input[i] = byte(i & 0xff)
		}

		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				MurmurHash3(input, 0)
			}
		})
	}
}
