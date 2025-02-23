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
package journal

import (
	"os"
	"supermassive/storage/hashtable"
	"testing"
)

func TestJournal(t *testing.T) {
	// Setup
	filePath := "test_journal.db"
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Test Append
	err = j.Append("key1", "value1", PUT)
	if err != nil {
		t.Errorf("Failed to append PUT operation: %v", err)
	}
	err = j.Append("key2", "", DEL)
	if err != nil {
		t.Errorf("Failed to append DEL operation: %v", err)
	}
	err = j.Append("key3", "0", PUT)
	if err != nil {
		t.Errorf("Failed to append INCR operation: %v", err)
	}
	err = j.Append("key4", "1", PUT)
	if err != nil {
		t.Errorf("Failed to append INCR operation: %v", err)
	}
	err = j.Append("key3", "1", INCR)
	if err != nil {
		t.Errorf("Failed to append INCR operation: %v", err)
	}
	err = j.Append("key4", "1", DECR)
	if err != nil {
		t.Errorf("Failed to append DECR operation: %v", err)
	}

	// Test Recover
	ht := hashtable.New()
	err = j.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover journal: %v", err)
	}

	// Verify hash table state
	value, _, ok := ht.Get("key1")
	if !ok || value != "value1" {
		t.Errorf("Expected key1 to have value 'value1', got %v", value)
	}
	_, _, ok = ht.Get("key2")
	if ok {
		t.Errorf("Expected key2 to be deleted")
	}
	value, _, ok = ht.Get("key3")
	if !ok || value != "1" {
		t.Errorf("Expected key3 to have value '1', got %v", value)
	}
	value, _, ok = ht.Get("key4")
	if !ok || value != "0" {
		t.Errorf("Expected key4 to have value '0', got %v", value)
	}
}
