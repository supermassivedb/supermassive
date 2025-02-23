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
	"fmt"
	"regexp"
	"strconv"
	"testing"
)

func TestNew(t *testing.T) {
	ht := New()
	if ht.size != 16 {
		t.Errorf("Expected initial size 16, got %d", ht.size)
	}
	if ht.growThreshold != 0.75 {
		t.Errorf("Expected growth threshold 0.75, got %f", ht.growThreshold)
	}
	if ht.shrinkThreshold != 0.25 {
		t.Errorf("Expected shrink threshold 0.25, got %f", ht.shrinkThreshold)
	}
}

func TestNewWithOptions(t *testing.T) {
	ht := NewWithOptions(32, 0.8, 0.2)
	if ht.size != 32 {
		t.Errorf("Expected size 32, got %d", ht.size)
	}
	if ht.growThreshold != 0.8 {
		t.Errorf("Expected growth threshold 0.8, got %f", ht.growThreshold)
	}
	if ht.shrinkThreshold != 0.2 {
		t.Errorf("Expected shrink threshold 0.2, got %f", ht.shrinkThreshold)
	}
}

func TestPut(t *testing.T) {
	ht := New()

	// Test basic insertion
	if !ht.Put("key1", "value1") {
		t.Error("Put failed for first insertion")
	}

	// Test update existing key
	if !ht.Put("key1", "value2") {
		t.Error("Put failed for update")
	}

	val, _, exists := ht.Get("key1")
	if !exists {
		t.Error("Key not found after Put")
	}
	if val != "value2" {
		t.Errorf("Expected value2, got %v", val)
	}
}

func TestGet(t *testing.T) {
	ht := New()

	// Test get non-existent key
	_, _, exists := ht.Get("nonexistent")
	if exists {
		t.Error("Get returned true for non-existent key")
	}

	// Test get existing key
	ht.Put("key1", "value1")
	val, _, exists := ht.Get("key1")
	if !exists {
		t.Error("Get returned false for existing key")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %v", val)
	}
}

func TestDelete(t *testing.T) {
	ht := New()

	// Test delete non-existent key
	if ht.Delete("nonexistent") {
		t.Error("Delete returned true for non-existent key")
	}

	// Test delete existing key
	ht.Put("key1", "value1")
	if !ht.Delete("key1") {
		t.Error("Delete returned false for existing key")
	}

	// Verify key was deleted
	_, _, exists := ht.Get("key1")
	if exists {
		t.Error("Key still exists after deletion")
	}
}

func TestTraverse(t *testing.T) {
	ht := New()

	// Add test data
	testData := map[string]int{
		"key1": 100,
		"key2": 200,
		"key3": 300,
	}

	for k, v := range testData {
		ht.Put(k, v)
	}

	// Test nil filter (get all entries)
	entries := ht.Traverse(nil)
	if len(entries) != len(testData) {
		t.Errorf("Expected %d entries, got %d", len(testData), len(entries))
	}

	// Test with filter
	greaterThan150 := func(entry Entry) bool {
		val, ok := entry.Value.(int)
		return ok && val > 150
	}

	filtered := ht.Traverse(greaterThan150)
	if len(filtered) != 2 { // key2 and key3 should match
		t.Errorf("Expected 2 entries > 150, got %d", len(filtered))
	}
}

func TestResizeGrow(t *testing.T) {
	ht := NewWithOptions(4, 0.75, 0.25)
	initialCapacity := ht.Capacity()

	// Add elements until resize
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key%d", i)
		ht.Put(key, i)
	}

	newCapacity := ht.Capacity()
	if newCapacity <= initialCapacity {
		t.Errorf("Expected capacity to grow from %d, got %d", initialCapacity, newCapacity)
	}

	// Verify all elements still accessible
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key%d", i)
		val, _, exists := ht.Get(key)
		if !exists {
			t.Errorf("Key %s not found after resize", key)
		}
		if val != i {
			t.Errorf("Expected %d, got %v", i, val)
		}
	}
}

func TestResizeShrink(t *testing.T) {
	ht := NewWithOptions(32, 0.75, 0.25)
	initialCapacity := ht.Capacity()

	// Add and then remove elements
	for i := 0; i < 20; i++ {
		ht.Put(strconv.Itoa(i), i)
	}

	// Remove most elements
	for i := 0; i < 15; i++ {
		ht.Delete(strconv.Itoa(i))
	}

	newCapacity := ht.Capacity()
	if newCapacity >= initialCapacity {
		t.Errorf("Expected capacity to shrink from %d, got %d", initialCapacity, newCapacity)
	}

	// Verify remaining elements still accessible
	for i := 15; i < 20; i++ {
		val, _, exists := ht.Get(strconv.Itoa(i))
		if !exists {
			t.Errorf("Key %d not found after resize", i)
		}
		if val != i {
			t.Errorf("Expected %d, got %v", i, val)
		}
	}
}

func TestGetWithRegex(t *testing.T) {
	ht := New()

	// Setup test data
	testData := map[string]string{
		"user:1":     "John",
		"user:2":     "Jane",
		"user:3":     "Bob",
		"product:1":  "Laptop",
		"product:2":  "Phone",
		"settings:1": "Dark Mode",
	}

	for k, v := range testData {
		ht.Put(k, v)
	}

	tests := []struct {
		name          string
		pattern       string
		limit         *int
		offset        *int
		expectedLen   int
		expectedError bool
	}{
		{
			name:          "Match all users",
			pattern:       "^user:\\d+$",
			limit:         nil,
			offset:        nil,
			expectedLen:   3,
			expectedError: false,
		},
		{
			name:          "Match all products with limit",
			pattern:       "^product:",
			limit:         &[]int{1}[0],
			offset:        nil,
			expectedLen:   1,
			expectedError: false,
		},
		{
			name:          "Match with offset",
			pattern:       "^user:",
			limit:         nil,
			offset:        &[]int{1}[0],
			expectedLen:   2,
			expectedError: false,
		},
		{
			name:          "Match with limit and offset",
			pattern:       "^user:",
			limit:         &[]int{1}[0],
			offset:        &[]int{1}[0],
			expectedLen:   1,
			expectedError: false,
		},
		{
			name:          "Invalid regex pattern",
			pattern:       "[",
			limit:         nil,
			offset:        nil,
			expectedLen:   0,
			expectedError: true,
		},
		{
			name:          "No matches",
			pattern:       "^nonexistent:",
			limit:         nil,
			offset:        nil,
			expectedLen:   0,
			expectedError: false,
		},
		{
			name:          "Match all with zero limit",
			pattern:       "^user:",
			limit:         &[]int{0}[0],
			offset:        nil,
			expectedLen:   0,
			expectedError: false,
		},
		{
			name:          "Match all with large offset",
			pattern:       "^user:",
			limit:         nil,
			offset:        &[]int{10}[0],
			expectedLen:   0,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := ht.GetWithRegex(tt.pattern, tt.limit, tt.offset)

			// Check error status
			if tt.expectedError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check results length
			if err == nil && len(results) != tt.expectedLen {
				t.Errorf("Expected %d results, got %d", tt.expectedLen, len(results))
			}

			// Additional checks for specific test cases
			if tt.name == "Match all users" && err == nil {
				for _, entry := range results {
					if matched, _ := regexp.MatchString("^user:\\d+$", entry.Key); !matched {
						t.Errorf("Unexpected key format: %s", entry.Key)
					}
				}
			}
		})
	}
}

func TestGetWithRegexEdgeCases(t *testing.T) {
	ht := New()

	// Test empty hash table
	results, err := ht.GetWithRegex(".*", nil, nil)
	if err != nil {
		t.Errorf("Unexpected error on empty hash table: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results from empty hash table, got %d", len(results))
	}

	// Test with special regex characters in keys
	specialKeys := map[string]string{
		"test.key":     "value1",
		"test*key":     "value2",
		"test[key]":    "value3",
		"test{1,2}key": "value4",
	}

	for k, v := range specialKeys {
		ht.Put(k, v)
	}

	// Test exact matches with escaped special characters
	for k := range specialKeys {
		pattern := regexp.QuoteMeta(k)
		results, err := ht.GetWithRegex(pattern, nil, nil)
		if err != nil {
			t.Errorf("Unexpected error matching special key %s: %v", k, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %s, got %d", k, len(results))
		}
	}

	// Test with very large limit
	largeLimit := 1000
	results, err = ht.GetWithRegex(".*", &largeLimit, nil)
	if err != nil {
		t.Errorf("Unexpected error with large limit: %v", err)
	}
	if len(results) != len(specialKeys) {
		t.Errorf("Expected %d results with large limit, got %d", len(specialKeys), len(results))
	}

	// Test with negative limit and offset
	negativeValue := -1
	_, err = ht.GetWithRegex(".*", &negativeValue, nil)
	if err != nil {
		t.Errorf("Unexpected error with negative limit: %v", err)
	}
	_, err = ht.GetWithRegex(".*", nil, &negativeValue)
	if err != nil {
		t.Errorf("Unexpected error with negative offset: %v", err)
	}
}

func BenchmarkGetWithRegex(b *testing.B) {
	ht := New()

	// Setup test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user:%d", i)
		ht.Put(key, fmt.Sprintf("value%d", i))
	}

	b.ResetTimer()

	b.Run("Simple Pattern", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ht.GetWithRegex("^user:[0-9]+$", nil, nil)
		}
	})

	b.Run("With Limit", func(b *testing.B) {
		limit := 10
		for i := 0; i < b.N; i++ {
			ht.GetWithRegex("^user:", &limit, nil)
		}
	})

	b.Run("With Offset", func(b *testing.B) {
		offset := 500
		for i := 0; i < b.N; i++ {
			ht.GetWithRegex("^user:", nil, &offset)
		}
	})

	b.Run("Complex Pattern", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ht.GetWithRegex("^user:([0-9]|[1-9][0-9]|[1-9][0-9][0-9])$", nil, nil)
		}
	})
}

func BenchmarkHashTable_Put(b *testing.B) {
	ht := New()
	for i := 0; i < b.N; i++ {
		ht.Put(string(rune('a'+(i%26))), i)
	}
}

func BenchmarkHashTable_Get(b *testing.B) {
	ht := New()
	for i := 0; i < 1000; i++ {
		ht.Put(string(rune('a'+(i%26))), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Get(string(rune('a' + (i % 26))))
	}
}

func BenchmarkHashTable_Delete(b *testing.B) {
	ht := New()
	for i := 0; i < 1000; i++ {
		ht.Put(string(rune('a'+(i%26))), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Delete(string(rune('a' + (i % 26))))
	}
}
