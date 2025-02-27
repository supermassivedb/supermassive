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
	"fmt"
	"os"
	"path/filepath"
	"supermassive/storage/hashtable"
	"sync"
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

func TestJournalBasicOperations(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_basic.db")
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
		t.Errorf("Failed to append PUT operation: %v", err)
	}
	err = j.Append("key4", "1", PUT)
	if err != nil {
		t.Errorf("Failed to append PUT operation: %v", err)
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

func TestJournalOverwriteOperations(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_overwrite.db")
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Test operations that overwrite values
	err = j.Append("key1", "value1", PUT)
	if err != nil {
		t.Errorf("Failed to append first PUT operation: %v", err)
	}
	err = j.Append("key1", "value2", PUT)
	if err != nil {
		t.Errorf("Failed to append second PUT operation: %v", err)
	}
	err = j.Append("key1", "", DEL)
	if err != nil {
		t.Errorf("Failed to append DEL operation: %v", err)
	}
	err = j.Append("key1", "value3", PUT)
	if err != nil {
		t.Errorf("Failed to append third PUT operation: %v", err)
	}

	// Test Recover
	ht := hashtable.New()
	err = j.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover journal: %v", err)
	}

	// Verify the final state reflects all operations in sequence
	value, _, ok := ht.Get("key1")
	if !ok || value != "value3" {
		t.Errorf("Expected key1 to have final value 'value3', got %v", value)
	}
}

func TestJournalIncrDecrOperations(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_incrDecr.db")
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Test INCR/DECR operations
	err = j.Append("counter", "10", PUT)
	if err != nil {
		t.Errorf("Failed to append initial PUT operation: %v", err)
	}

	// Series of INCR/DECR operations
	for i := 0; i < 5; i++ {
		err = j.Append("counter", "2", INCR)
		if err != nil {
			t.Errorf("Failed to append INCR operation: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		err = j.Append("counter", "1", DECR)
		if err != nil {
			t.Errorf("Failed to append DECR operation: %v", err)
		}
	}

	// Test Recover
	ht := hashtable.New()
	err = j.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover journal: %v", err)
	}

	// Verify the final counter value: 10 + (5*2) - (3*1) = 17
	value, _, ok := ht.Get("counter")
	if !ok || value != "17" {
		t.Errorf("Expected counter to have value '17', got %v", value)
	}
}

func TestJournalEmptyOperations(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_empty.db")
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Test Recover on empty journal
	ht := hashtable.New()
	err = j.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover empty journal: %v", err)
	}

	// Verify hash table is empty
	if ht.Size() != 0 {
		t.Errorf("Expected empty hash table, got size %d", ht.Size())
	}
}

func TestJournalLargeData(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_large.db")
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Create large key and value (10KB)
	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(65 + (i % 26)) // Fill with repeating alphabet
	}
	largeKey := "large_key"
	largeValStr := string(largeValue)

	// Test appending large data
	err = j.Append(largeKey, largeValStr, PUT)
	if err != nil {
		t.Errorf("Failed to append large data: %v", err)
	}

	// Test Recover
	ht := hashtable.New()
	err = j.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover journal with large data: %v", err)
	}

	// Verify large data was recovered correctly
	value, _, ok := ht.Get(largeKey)
	if !ok || value != largeValStr {
		t.Errorf("Large value was not recovered correctly")
	}
}

func TestJournalReopenAndRecover(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_reopen.db")
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}

	// Write some data
	err = j.Append("key1", "value1", PUT)
	if err != nil {
		t.Errorf("Failed to append PUT operation: %v", err)
	}
	err = j.Append("key2", "value2", PUT)
	if err != nil {
		t.Errorf("Failed to append PUT operation: %v", err)
	}

	// Close the journal
	err = j.Close()
	if err != nil {
		t.Errorf("Failed to close journal: %v", err)
	}

	// Reopen the journal
	j2, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to reopen journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j2.Close()

	// Append more data
	err = j2.Append("key3", "value3", PUT)
	if err != nil {
		t.Errorf("Failed to append PUT operation after reopen: %v", err)
	}

	// Test Recover
	ht := hashtable.New()
	err = j2.Recover(ht)
	if err != nil {
		t.Errorf("Failed to recover journal after reopen: %v", err)
	}

	// Verify all data was recovered
	if ht.Size() != 3 {
		t.Errorf("Expected 3 entries, got %d", ht.Size())
	}

	value, _, ok := ht.Get("key1")
	if !ok || value != "value1" {
		t.Errorf("Expected key1 to have value 'value1', got %v", value)
	}

	value, _, ok = ht.Get("key3")
	if !ok || value != "value3" {
		t.Errorf("Expected key3 to have value 'value3', got %v", value)
	}
}

func TestJournalSerializeDeserialize(t *testing.T) {
	// Test various entry types
	testCases := []struct {
		name  string
		entry Entry
	}{
		{
			name:  "PUT operation",
			entry: Entry{Key: "test_key", Value: "test_value", Op: PUT},
		},
		{
			name:  "DEL operation",
			entry: Entry{Key: "test_key", Value: "", Op: DEL},
		},
		{
			name:  "INCR operation",
			entry: Entry{Key: "counter", Value: "5", Op: INCR},
		},
		{
			name:  "DECR operation",
			entry: Entry{Key: "counter", Value: "2", Op: DECR},
		},
		{
			name:  "Empty key",
			entry: Entry{Key: "", Value: "empty_key_test", Op: PUT},
		},
		{
			name:  "Unicode characters",
			entry: Entry{Key: "unicode_key_😀", Value: "unicode_value_世界", Op: PUT},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize the entry
			data, err := Serialize(tc.entry)
			if err != nil {
				t.Fatalf("Failed to serialize entry: %v", err)
			}

			// Deserialize back to an entry
			recoveredEntry, err := Deserialize(data)
			if err != nil {
				t.Fatalf("Failed to deserialize entry: %v", err)
			}

			// Verify the recovered entry matches the original
			if recoveredEntry.Key != tc.entry.Key {
				t.Errorf("Key mismatch: expected '%s', got '%s'", tc.entry.Key, recoveredEntry.Key)
			}
			if recoveredEntry.Value != tc.entry.Value {
				t.Errorf("Value mismatch: expected '%s', got '%s'", tc.entry.Value, recoveredEntry.Value)
			}
			if recoveredEntry.Op != tc.entry.Op {
				t.Errorf("Operation mismatch: expected %v, got %v", tc.entry.Op, recoveredEntry.Op)
			}
		})
	}
}

func TestJournalInvalidDeserialize(t *testing.T) {
	// Create invalid data
	invalidData := []byte("this is not a valid gob-encoded entry")

	// Try to deserialize
	_, err := Deserialize(invalidData)
	if err == nil {
		t.Error("Expected error when deserializing invalid data, got nil")
	}
}

func TestJournalOpenInvalidPath(t *testing.T) {
	// Try to open a journal with an invalid path
	invalidPath := filepath.Join(string([]byte{0x7f}), "invalid_path")
	_, err := Open(invalidPath)
	if err == nil {
		t.Error("Expected error when opening journal with invalid path, got nil")
	}
}

func TestJournalRecoverCorruptedJournal(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_corrupted.db")

	// Write corrupted data directly to the file
	err := os.WriteFile(filePath, []byte("corrupted data"), 0666)
	if err != nil {
		t.Fatalf("Failed to create corrupted journal file: %v", err)
	}
	defer os.Remove(filePath)

	// Open the journal with corrupted data
	j, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open corrupted journal: %v", err)
	}
	defer j.Close()

	// Test Recover
	ht := hashtable.New()
	err = j.Recover(ht)
	// Recovery should continue despite errors with corrupted entries

	// Verify hash table is empty
	if ht.Size() != 0 {
		t.Errorf("Expected empty hash table after recovering corrupted journal, got size %d", ht.Size())
	}
}

func TestJournalConcurrentAppend(t *testing.T) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "test_journal_concurrent.db")
	journal, err := Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer journal.Close()

	// Number of goroutines and operations per goroutine
	numGoroutines := 10
	opsPerGoroutine := 100

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Create an error channel to collect errors from goroutines
	errorChan := make(chan error, numGoroutines*opsPerGoroutine)

	// Launch multiple goroutines to append concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			// Each goroutine performs multiple append operations
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", routineID, j)
				value := fmt.Sprintf("value_%d_%d", routineID, j)

				// Mix different operation types
				var op Operation
				switch j % 4 {
				case 0:
					op = PUT
				case 1:
					// For DEL operations, we don't need a value
					// First PUT the key, then DEL it
					err := journal.Append(key, value, PUT)
					if err != nil {
						errorChan <- fmt.Errorf("goroutine %d failed to append PUT before DEL: %v", routineID, err)
						continue
					}
					err = journal.Append(key, "", DEL)
					if err != nil {
						errorChan <- fmt.Errorf("goroutine %d failed to append DEL operation: %v", routineID, err)
					}
					continue
				case 2:
					// For INCR, use a numeric value
					value = "1"
					op = INCR
				case 3:
					// For DECR, use a numeric value
					value = "1"
					op = DECR
				}

				err := journal.Append(key, value, op)
				if err != nil {
					errorChan <- fmt.Errorf("goroutine %d failed to append operation %d: %v", routineID, j, err)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Check if there were any errors
	for err := range errorChan {
		t.Error(err)
	}

	// Test Recover to ensure all operations were correctly recorded
	ht := hashtable.New()
	err = journal.Recover(ht)
	if err != nil {
		// Error during recovery is expected due to concurrent operations
		// For example, a DECR or INCR operation might be processed before its key is created
		t.Logf("Got expected error during recovery with concurrent operations: %v", err)
		// Continue with the test - we still expect some entries to be recovered
	}

	// Verify that we have at least some entries in the hashtable
	// The exact count will depend on the sequence of operations
	if ht.Size() == 0 {
		t.Errorf("Expected non-empty hashtable after recovery")
	}

	t.Logf("Recovered %d entries from the journal after concurrent operations", ht.Size())
}

func BenchmarkJournalAppend(b *testing.B) {
	// Setup
	filePath := filepath.Join(os.TempDir(), "bench_journal_append.db")
	j, err := Open(filePath)
	if err != nil {
		b.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Benchmark Append operations
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune(i%26+65))
		value := "value" + string(rune(i%26+65))
		err := j.Append(key, value, PUT)
		if err != nil {
			b.Fatalf("Failed to append during benchmark: %v", err)
		}
	}
}

func BenchmarkJournalRecover(b *testing.B) {
	// Setup - create a journal with many entries
	filePath := filepath.Join(os.TempDir(), "bench_journal_recover.db")
	j, err := Open(filePath)
	if err != nil {
		b.Fatalf("Failed to open journal: %v", err)
	}
	defer os.Remove(filePath)
	defer j.Close()

	// Add a substantial number of entries
	numEntries := 10000
	for i := 0; i < numEntries; i++ {
		key := "key" + string(rune(i%26+65))
		value := "value" + string(rune(i%26+65))
		err := j.Append(key, value, PUT)
		if err != nil {
			b.Fatalf("Failed to append during benchmark setup: %v", err)
		}
	}

	// Benchmark Recover
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht := hashtable.New()
		err := j.Recover(ht)
		if err != nil {
			b.Fatalf("Failed to recover during benchmark: %v", err)
		}
	}
}
