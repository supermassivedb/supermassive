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
