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
package pager

// Append only file pager
// We essentially only care about appending data to the file but keeping each page equal in size.
// The pager handles overflowing data by creating new pages and linking them together, if need be.
// The iterator is able to traverse through the pages reliable skipping and gathering pages as needed.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Pager is the main pager struct
type Pager struct {
	file         *os.File        // File to use for paging
	pageSize     int             // Size of each page.. if data overflows new pages are created and linked
	syncQuit     chan struct{}   // Channel to quit background fsync
	wg           *sync.WaitGroup // WaitGroup for background fsync
	syncInterval time.Duration   // File sync interval
	sync         bool            // To sync or not to sync
	closed       atomic.Bool     // We use to track if we have closed the pager already to prevent double closing
}

// Iterator is the iterator struct used for
// iterator through pages within paged file
type Iterator struct {
	pager       *Pager // Pager for iterator
	pageStack   []int  // Stack of page numbers
	currentPage int    // Current page number
	CurrentData []byte // Current data
	maxPages    int    // Max pages based on file size calculation
}

// Open opens a file for paging
func Open(filename string, flag int, perm os.FileMode, pageSize int, syncOn bool, syncInterval time.Duration) (*Pager, error) {
	var err error
	pager := &Pager{pageSize: pageSize, syncQuit: make(chan struct{}), wg: &sync.WaitGroup{}, syncInterval: syncInterval, sync: syncOn}

	// Open the file for reading and writing
	pager.file, err = os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	if !pager.sync {
		return pager, nil
	}

	// Initialize closed to false
	pager.closed.Store(false)

	// Start background sync
	pager.wg.Add(1)
	go pager.backgroundSync()

	return pager, nil
}

// Close closes the pager gracefully
func (p *Pager) Close() error {
	if p == nil {
		return nil
	}

	if p.file == nil {
		return nil
	}

	// Only close channel if sync is enabled and we haven't closed before
	if p.sync && !p.closed.Swap(true) {
		close(p.syncQuit)
		p.wg.Wait()
	}

	return p.file.Close()
}

// Truncate truncates the file
func (p *Pager) Truncate() error {
	if err := p.file.Truncate(0); err != nil {
		return err
	}
	return nil
}

// Size returns the size of the file
func (p *Pager) Size() int64 {
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return fileInfo.Size()
}

// backgroundSync is a goroutine that syncs the file in the background every syncInterval
func (p *Pager) backgroundSync() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = p.file.Sync()
		case <-p.syncQuit:
			_ = p.file.Sync() // Escalate then return..
			return
		}
	}
}

// chunk splits a byte slice into n chunks
func chunk(data []byte, n int) ([][]byte, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0")
	}

	// Calculate the chunk size
	chunkSize := int(math.Ceil(float64(len(data)) / float64(n)))

	var chunks [][]byte

	// Loop to slice the data into chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	return chunks, nil
}

// Write writes data to the pager
func (p *Pager) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return -1, errors.New("data is empty")
	}

	initialPgN := -1

	if len(data) > p.pageSize {
		chunks, err := chunk(data, p.pageSize)
		if err != nil {
			return -1, err
		}

		// Each chunk is a page
		for i, c := range chunks {

			if i == len(chunks)-1 {
				if _, err := p.writePage(c, false); err != nil {
					return -1, err
				}
				break
			}

			if pg, err := p.writePage(c, true); err != nil {
				return -1, err
			} else {

				if initialPgN == -1 {
					initialPgN = pg
				}
			}
		}
		return initialPgN, nil
	}

	return p.writePage(data, false)
}

// GetPageSize returns the page size
func (p *Pager) GetPageSize() int {
	return p.pageSize
}

// writePage writes a page to the file
func (p *Pager) writePage(data []byte, overflow bool) (int, error) {
	pageNumber := p.newPageNumber()

	// Create a buffer to hold the header and the data, ensuring it is the size of a page
	buffer := make([]byte, p.pageSize+16)

	// Write the size of the data (int64) to the buffer
	binary.LittleEndian.PutUint64(buffer[0:], uint64(len(data)))

	// Write the overflow flag (int64) to the buffer
	if overflow {
		binary.LittleEndian.PutUint64(buffer[8:], 1)
	} else {
		binary.LittleEndian.PutUint64(buffer[8:], 0)
	}

	// Write the actual data to the buffer, ensuring it does not exceed the page size
	copy(buffer[16:], data)

	// write to end of file
	// we seek to the end of the file
	_, err := p.file.Seek(0, 2)
	if err != nil {
		return 0, err
	}

	_, err = p.file.Write(buffer)
	if err != nil {
		return -1, err
	}
	return int(pageNumber), nil
}

// newPageNumber returns the next page number
func (p *Pager) newPageNumber() int64 {
	// Get the file size
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return fileInfo.Size() / int64(p.pageSize+16) // We add 16 bytes for the page header
}

// Read reads a page from the file
func (p *Pager) Read(pg int) ([]byte, int, error) {
	var data []byte

	for {
		// Seek to the start of the page
		offset := int64(pg) * int64(p.pageSize+16)
		_, err := p.file.Seek(offset, 0)
		if err != nil {
			return nil, -1, err
		}

		// Read the header
		header := make([]byte, 16)
		_, err = p.file.Read(header)
		if err != nil {
			return nil, -1, err
		}

		// Get the size of the data
		dataSize := binary.LittleEndian.Uint64(header[0:8])

		// Read the data
		pageData := make([]byte, dataSize)
		_, err = p.file.Read(pageData)
		if err != nil {
			return nil, -1, err
		}

		// Append the data to the result
		data = append(data, pageData...)

		// Check the overflow flag
		overflow := binary.LittleEndian.Uint64(header[8:16])
		if overflow == 0 {
			break
		}

		// Move to the next page
		pg++
	}
	// We return the last page number read
	return data, pg, nil
}

// PageCount returns the number of pages in the file
func (p *Pager) PageCount() int {
	// We could use an iterator and gather a better count but this works as well..
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return int(fileInfo.Size()) / (p.pageSize + 16)
}

// Name returns the name of the file
func (p *Pager) Name() string {
	return p.file.Name()

}

// NewIterator returns a new iterator
func NewIterator(pager *Pager) *Iterator {
	return &Iterator{maxPages: pager.PageCount(), pager: pager, currentPage: 0}
}

// Next moves the iterator to the next page
func (it *Iterator) Next() bool {
	if it.currentPage < it.maxPages {
		it.stackAdd(it.currentPage)
		read, lastPg, err := it.pager.Read(it.currentPage)
		if err != nil {
			return false
		}

		it.currentPage = lastPg + 1
		it.CurrentData = read
		return true
	}
	return false
}

// Prev moves the iterator to the previous page
func (it *Iterator) Prev() bool {
	if len(it.pageStack) == 0 {
		return false
	}

	// Pop the last page number from the stack
	it.currentPage = it.pageStack[len(it.pageStack)-1]
	it.pageStack = it.pageStack[:len(it.pageStack)-1]

	// Read the data for the current page
	read, _, err := it.pager.Read(it.currentPage)
	if err != nil {
		return false
	}

	it.CurrentData = read
	return true
}

// Read reads the current page
func (it *Iterator) Read() ([]byte, error) {
	return it.CurrentData, nil
}

// stackAdd adds a page number to the stack
func (it *Iterator) stackAdd(pg int) {
	// We avoid adding the same page number to the stack
	for _, p := range it.pageStack {
		if p == pg {
			return
		}
	}

	it.pageStack = append(it.pageStack, pg)
}

// FileName returns the pager underlying file name
func (p *Pager) FileName() string {
	return p.file.Name()
}

// EscalateFSync escalates a disk fsync
func (p *Pager) EscalateFSync() {
	_ = p.file.Sync() // Is thread safe
}

// LastPage returns the last page number
func (p *Pager) LastPage() int {
	// Create an iterator
	it := NewIterator(p)
	lastValidPage := -1

	// Iterate through all pages
	for it.Next() {
		// The iterator's currentPage is already incremented to the next page
		// We want the page we just read, which is stored in the stack
		if len(it.pageStack) > 0 {
			lastValidPage = it.pageStack[len(it.pageStack)-1]
		}
	}

	return lastValidPage
}

func NewIteratorAtPage(pager *Pager, startPage int) (*Iterator, error) {
	// Validate the start page
	if startPage < 0 {
		return nil, fmt.Errorf("invalid start page: must be >= 0")
	}

	maxPages := pager.PageCount()
	if startPage >= maxPages {
		return nil, fmt.Errorf("start page %d exceeds maximum pages %d", startPage, maxPages)
	}

	// Create iterator with specified start page
	it := &Iterator{
		pager:       pager,
		currentPage: startPage,
		maxPages:    maxPages,
		pageStack:   make([]int, 0),
	}

	// Initialize the iterator's current data by reading the start page
	read, _, err := pager.Read(startPage)
	if err != nil {
		return nil, fmt.Errorf("failed to read start page: %w", err)
	}
	it.CurrentData = read

	return it, nil
}

// Stats returns statistics about the pager
func (p *Pager) Stats() map[string]string {
	stats := make(map[string]string)

	// Basic file information
	fileInfo, err := p.file.Stat()
	if err == nil {
		stats["file_name"] = fileInfo.Name()
		stats["file_size"] = fmt.Sprintf("%d", fileInfo.Size())
		stats["file_mode"] = fileInfo.Mode().String()
		stats["modified_time"] = fileInfo.ModTime().Format(time.RFC3339)
	}

	// Pager configuration
	stats["page_size"] = fmt.Sprintf("%d", p.pageSize)
	stats["sync_enabled"] = fmt.Sprintf("%t", p.sync)
	stats["sync_interval"] = p.syncInterval.String()
	stats["is_closed"] = fmt.Sprintf("%t", p.closed.Load())

	// Page statistics
	totalPages := p.PageCount()
	stats["total_pages"] = fmt.Sprintf("%d", totalPages)
	stats["last_page"] = fmt.Sprintf("%d", p.LastPage())

	// Storage efficiency
	headerSize := int64(16) // 8 bytes for data size + 8 bytes for overflow flag
	totalHeaderSize := headerSize * int64(totalPages)
	totalStorageSize := fileInfo.Size()
	dataSize := totalStorageSize - totalHeaderSize

	stats["total_header_size"] = fmt.Sprintf("%d", totalHeaderSize)
	stats["total_data_size"] = fmt.Sprintf("%d", dataSize)
	stats["storage_efficiency"] = fmt.Sprintf("%.4f", float64(dataSize)/float64(totalStorageSize))

	// Calculate average page utilization
	if totalPages > 0 {
		avgPageSize := float64(dataSize) / float64(totalPages)
		pageUtilization := avgPageSize / float64(p.pageSize)
		stats["avg_page_size"] = fmt.Sprintf("%.2f", avgPageSize)
		stats["page_utilization"] = fmt.Sprintf("%.4f", pageUtilization)
	} else {
		stats["avg_page_size"] = "0"
		stats["page_utilization"] = "0"
	}

	// Performance indicators
	overhead := float64(totalHeaderSize) / float64(totalStorageSize)
	stats["header_overhead_ratio"] = fmt.Sprintf("%.4f", overhead)

	return stats
}
