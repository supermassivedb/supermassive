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
package nodereplica

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"supermassive/journal"
	"supermassive/network/server"
	"supermassive/storage/hashtable"
	"supermassive/utility"
	"sync"
	"time"
)

// ConfigFile is the node replica configuration file
const ConfigFile = ".nodereplica"

// JournalFile is the node replica journal file
const JournalFile = ".journal"

// Config is the node configurations
type Config struct {
	MaxMemoryThreshold uint64         `yaml:"max-memory-threshold"` // Max memory threshold for the node replica
	ServerConfig       *server.Config `yaml:"server-config"`        // Node replica server configs
}

// NodeReplica is the main struct for the node replica
type NodeReplica struct {
	Config    *Config              // Is the node replica configuration
	Server    *server.Server       // Is the node replica server
	Logger    *slog.Logger         // Is the logger for the node replica
	SharedKey string               // Is the shared key for the node replica
	Storage   *hashtable.HashTable // Is the storage for the node replica
	Journal   *journal.Journal     // Is the journal for the node replica
	Lock      *sync.RWMutex        // Is the lock for the node replica
	MaxMemory uint64               // Is the max memory for the system

}

// ServerConnectionHandler is the handler for the server connections
type ServerConnectionHandler struct {
	NodeReplica *NodeReplica // Node replica instance
	BufferSize  int          // Defined buffer size for the handler
	ReadTimeout int          // Defined read timeout for the handler
}

// New creates a new node replica instance
func New(logger *slog.Logger, sharedKey string) (*NodeReplica, error) {
	if logger == nil {
		return nil, errors.New("logger is required")
	}

	if sharedKey == "" {
		return nil, errors.New("shared key is required")
	}

	maxMem, err := utility.GetMaxMemory()
	if err != nil {
		return nil, err
	}

	return &NodeReplica{Logger: logger, SharedKey: sharedKey, Storage: hashtable.New(), Lock: &sync.RWMutex{}, MaxMemory: maxMem}, nil
}

// Open opens a new node replica instance
func (nr *NodeReplica) Open() error {
	// We get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Config for node replica
	var conf *Config

	// We check if the config file exists
	if _, err := os.Stat(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), ConfigFile)); os.IsNotExist(err) {
		// If it does not exist, we create a default config file
		conf, err = createDefaultConfigFile(wd)
		if err != nil {
			return err
		}

	} else {
		// If it exists, we open the existing config file
		conf, err = openExistingConfigFile(wd)
		if err != nil {
			return err
		}

	}

	// Set the node replica configuration
	nr.Config = conf

	// We create a new server
	nr.Server = server.New(nr.Config.ServerConfig, nr.Logger, &ServerConnectionHandler{
		NodeReplica: nr,
		BufferSize:  nr.Config.ServerConfig.BufferSize,
		ReadTimeout: nr.Config.ServerConfig.ReadTimeout,
	})

	nr.Journal, err = journal.Open(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), JournalFile))
	if err != nil {
		return err
	}

	// We start the server
	err = nr.Server.Start()
	if err != nil {
		return err
	}

	return nil
}

// Close closes the node replica instance gracefully
func (nr *NodeReplica) Close() error {
	// We close the server
	err := nr.Server.Shutdown()
	if err != nil {
		return err
	}

	err = nr.Journal.Close()
	if err != nil {
		return err
	}

	return nil
}

// openExistingConfigFile opens an existing node replica config file
func openExistingConfigFile(wd string) (*Config, error) {

	// If it exists, we read the config file
	f, err := os.Open(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), ConfigFile))
	if err != nil {
		return nil, err
	}

	defer f.Close()

	// We decode the config file
	decoder := yaml.NewDecoder(f)
	config := &Config{}
	err = decoder.Decode(config)

	if err != nil {
		return nil, err
	}

	return config, nil
}

// createDefaultConfigFile creates a default node replica config file
func createDefaultConfigFile(wd string) (*Config, error) {
	// We create the default node replica config file
	f, err := os.Create(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), ConfigFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	config := &Config{
		ServerConfig: &server.Config{
			Address:     "localhost:4002",
			UseTLS:      false,
			CertFile:    "/",
			KeyFile:     "/",
			ReadTimeout: 10,
			BufferSize:  1024,
		},
	}

	// We marshal the config to yaml
	data, err := yaml.Marshal(config)
	if err != nil {
		return nil, err
	}

	// We write the config to the file
	_, err = f.Write(data)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// HandleConnection handles the server connections
func (h *ServerConnectionHandler) HandleConnection(conn net.Conn) {
	// Create a buffer for receiving data
	buffer := make([]byte, h.BufferSize)
	var tempBuffer []byte // Temporary buffer to store data (larger than buffer)

	authenticated := false // Is the connection authenticated

	for {

		_ = conn.SetReadDeadline(time.Time{})
		n, err := conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				h.NodeReplica.Logger.Warn("connection timeout", "remote_addr", conn.RemoteAddr())
			} else {
				h.NodeReplica.Logger.Warn("read error", "error", err, "remote_addr", conn.RemoteAddr())
			}
			return
		}

		// Append the read data to the temporary buffer
		tempBuffer = append(tempBuffer, buffer[:n]...)

		// Check if the command is complete (ends with \r\n)
		if !bytes.HasSuffix(tempBuffer, []byte("\r\n")) {
			continue
		}

		// Process the complete command
		command := tempBuffer
		tempBuffer = nil // Reset the temporary buffer for the next command

		command = bytes.TrimSuffix(command, []byte("\r\n"))

		// We print and send back the command
		h.NodeReplica.Logger.Info("received command", "command", string(command))

		switch {
		case strings.HasPrefix(string(command), "NAUTH"):

			if !authenticated {

				sharedKey := strings.Split(string(command), " ")[1]

				// We hash the shared key
				sharedKeyHash := sha256.Sum256([]byte(h.NodeReplica.SharedKey))

				// We compare the shared key hash
				if fmt.Sprintf("%x", sharedKeyHash) == sharedKey {
					_, err = conn.Write([]byte("OK authenticated\r\n"))
					if err != nil {
						h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}

					authenticated = true

					continue
				} else {
					_, err = conn.Write([]byte("ERR invalid key\r\n"))
					if err != nil {
						h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}
				}
			} else {
				_, err = conn.Write([]byte("ERR already authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "STARTSYNC"):

			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			h.NodeReplica.Lock.Lock()

			// Because this is a replica we send over SYNCFROM PG <last journal page number>
			// We know the connected should be a primary node
			// The primary will now send us missing pages
			_, err = conn.Write([]byte(fmt.Sprintf("SYNCFROM %d\r\n", h.NodeReplica.Journal.Pager.LastPage())))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				h.NodeReplica.Lock.Unlock()
				return
			}

			h.NodeReplica.Lock.Unlock()

		case strings.HasPrefix(string(command), "DONESYNC"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We are done syncing with primary
			// We log it
			h.NodeReplica.Logger.Info("synced with primary", "remote_addr", conn.RemoteAddr())

		case strings.HasPrefix(string(command), "PING"):
			_, err = conn.Write([]byte("OK PONG\r\n"))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "REGX"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// Check for optional offset and limit
			// Can be REGX <pattern> <offset> <limit>
			// or REGX <pattern>
			var offset, limit int
			if len(strings.Split(string(command), " ")) > 2 {
				offset, _ = strconv.Atoi(strings.Split(string(command), " ")[2])
				limit, _ = strconv.Atoi(strings.Split(string(command), " ")[3])
			}

			pattern := strings.Split(string(command), " ")[1]

			var results [][]byte

			// We acquire read lock
			h.NodeReplica.Lock.RLock()
			entries, err := h.NodeReplica.Storage.GetWithRegex(pattern, &offset, &limit)
			if err != nil {
				_, err = conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.NodeReplica.Lock.RUnlock()
					return
				}
				h.NodeReplica.Lock.RUnlock()
				return
			}

			for i, entry := range entries {
				if i == 0 {
					results = append(results, []byte(fmt.Sprintf("OK %s %s %s\r\n", entry.Timestamp.Format(time.RFC3339), entry.Key, entry.Value)))
				} else {
					results = append(results, []byte(fmt.Sprintf("%s %s %s\r\n", entry.Timestamp.Format(time.RFC3339), entry.Key, entry.Value)))
				}

			}

			// We release read lock
			h.NodeReplica.Lock.RUnlock()
			// We join all results into a single byte slice
			_, err = conn.Write(bytes.Join(results, []byte("")))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "PUT"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			if h.NodeReplica.MemoryCheck() == false {
				// We are out of memory
				_, err = conn.Write([]byte("ERR out of memory\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We put the data
			key := strings.Split(string(command), " ")[1]
			value := strings.Join(strings.Split(string(command), " ")[2:], " ")

			h.NodeReplica.Lock.Lock()
			h.NodeReplica.Storage.Put(key, value)
			h.NodeReplica.Lock.Unlock()

			go func() {
				err := h.NodeReplica.Journal.Append(key, value, journal.PUT)
				if err != nil {
					h.NodeReplica.Logger.Warn("journal append error", "error", err)
				}
			}()

			_, err = conn.Write([]byte("OK key-value written\r\n"))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "GET"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We get the data
			key := strings.Split(string(command), " ")[1]
			h.NodeReplica.Lock.RLock()
			value, ts, ok := h.NodeReplica.Storage.Get(key)
			h.NodeReplica.Lock.RUnlock()

			if ok {
				// Format time in RFC3339
				// OK 2021-09-01T12:00:00Z key value
				_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, value)))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {
				_, err = conn.Write([]byte("ERR key not found\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "DEL"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We delete the data
			key := strings.Split(string(command), " ")[1]

			h.NodeReplica.Lock.Lock()
			ok := h.NodeReplica.Storage.Delete(key)
			h.NodeReplica.Lock.Unlock()

			if ok {

				go func() {
					err := h.NodeReplica.Journal.Append(key, "", journal.DEL)
					if err != nil {
						h.NodeReplica.Logger.Warn("journal append error", "error", err)
					}
				}()

				_, err = conn.Write([]byte("OK key-value deleted\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {
				_, err = conn.Write([]byte("ERR key-value not found\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "INCR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			key := strings.Split(string(command), " ")[1] // We get incrementing key

			// We check if we have incrementing value
			if len(strings.Split(string(command), " ")) < 3 {
				_, err := conn.Write([]byte("ERR invalid value\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				return
			}
			h.NodeReplica.Lock.Lock()
			val, ts, err := h.NodeReplica.Storage.Incr(key, strings.Split(string(command), " ")[2])
			if err != nil {
				_, err := conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.NodeReplica.Lock.Unlock()
					return
				}
				h.NodeReplica.Lock.Unlock()
				return
			}

			h.NodeReplica.Lock.Unlock()

			go func() {
				err = h.NodeReplica.Journal.Append(key, strings.Split(string(command), " ")[2], journal.PUT)
				if err != nil {
					h.NodeReplica.Logger.Warn("journal append error", "error", err)
				}
			}()

			_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, val)))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "DECR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			key := strings.Split(string(command), " ")[1] // We get decrementing key

			// We check if we have a decrementing value
			if len(strings.Split(string(command), " ")) < 3 {
				_, err := conn.Write([]byte("ERR invalid value\r\n"))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				return
			}

			h.NodeReplica.Lock.Lock()

			val, ts, err := h.NodeReplica.Storage.Decr(key, strings.Split(string(command), " ")[2])
			if err != nil {
				_, err := conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.NodeReplica.Lock.Unlock()
					return
				}
				h.NodeReplica.Lock.Unlock()
				return
			}

			h.NodeReplica.Lock.Unlock()

			err = h.NodeReplica.Journal.Append(key, strings.Split(string(command), " ")[2], journal.PUT)
			if err != nil {
				h.NodeReplica.Logger.Warn("journal append error", "error", err)
			}

			_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, val)))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "QUIT"):
			_, err = conn.Write([]byte("OK see ya later\r\n"))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

			return
		default:
			_, err = conn.Write([]byte("ERR unknown command\r\n"))
			if err != nil {
				h.NodeReplica.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		}

	}
}

// MemoryCheck checks the memory usage of the node replica
func (nr *NodeReplica) MemoryCheck() bool {
	// Get the current memory usage
	currentMemoryUsage := utility.GetCurrentMemoryUsage()

	// Calculate the percentage of current memory usage relative to nr.MaxMemory
	memoryUsagePercentage := (float64(currentMemoryUsage) / float64(nr.MaxMemory)) * 100

	// We compare it with the MaxMemoryThreshold set in Config
	if memoryUsagePercentage > float64(nr.Config.MaxMemoryThreshold) {
		return true
	}

	return false
}
