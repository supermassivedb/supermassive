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
	"bytes"
	"encoding/gob"
	"os"
	"supermassive/storage/hashtable"
	"supermassive/storage/pager"
	"sync"
	"time"
)

// Operation is a journal operation
type Operation int

const (
	PUT Operation = iota
	DEL
	INCR
	DECR
)

// Entry is a journal entry
type Entry struct {
	Key   string
	Value string
	Op    Operation
}

// Journal is a journal for node and node-replica instances
// Used to store PUT, DEL, INCR, DECR operations, and recover the state of the hashtable on startup if configured
type Journal struct {
	Pager *pager.Pager
	Lock  *sync.Mutex
}

// Open opens a journal file
func Open(filePath string) (*Journal, error) {
	p, err := pager.Open(filePath, os.O_CREATE|os.O_RDWR, 0777, 1024, true, time.Millisecond*128)
	if err != nil {
		return nil, err
	}

	return &Journal{Pager: p, Lock: &sync.Mutex{}}, nil
}

// Close closes the journal file
func (j *Journal) Close() error {
	return j.Pager.Close()
}

// Append appends an entry to the journal file
func (j *Journal) Append(key, value string, op Operation) error {
	e := Entry{Key: key, Value: value, Op: op}

	b, err := Serialize(e)
	if err != nil {
		return err
	}

	j.Lock.Lock()
	defer j.Lock.Unlock()

	_, err = j.Pager.Write(b)
	if err != nil {
		return err
	}

	return nil
}

// Recover reads the journal file and replays the operations to an in-memory hash table
func (j *Journal) Recover(ht *hashtable.HashTable) error {
	it := pager.NewIterator(j.Pager)
	for it.Next() {
		data, err := it.Read()
		if err != nil {
			break
		}

		e, err := Deserialize(data)
		if err != nil {
			continue
		}

		switch e.Op {
		case PUT:
			ht.Put(e.Key, e.Value)
		case DEL:
			ht.Delete(e.Key)
		case INCR:
			_, _, err := ht.Incr(e.Key, e.Value)
			if err != nil {
				return err
			}
		case DECR:
			_, _, err := ht.Decr(e.Key, e.Value)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// serialize serializes an Entry into a byte slice
func Serialize(e Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(e)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// deserialize deserializes a byte slice into an Entry
func Deserialize(b []byte) (*Entry, error) {
	var e Entry
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&e)
	if err != nil {
		return nil, err
	}
	return &e, nil
}
