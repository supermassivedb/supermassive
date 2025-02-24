// BSD 3-Clause License
//
// (C) Copyright 2025,  Alex Gaetano Padula & SuperMassive authors
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
package node

import (
	"bytes"
	"context"
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
	"supermassive/network/client"
	"supermassive/network/server"
	"supermassive/storage/hashtable"
	"supermassive/storage/pager"
	"supermassive/utility"
	"sync"
	"time"
)

// ConfigFile is the node configuration file
const ConfigFile = ".node"

// JournalFile is the journal file for this node
const JournalFile = ".journal"

// Config is the node configurations
type Config struct {
	HealthCheckInterval int              `yaml:"health-check-interval"` // Health check interval
	MaxMemoryThreshold  uint64           `yaml:"max-memory-threshold"`  // Maximum memory threshold, default 75% of system memory
	ServerConfig        *server.Config   `yaml:"server-config"`         // Node server configs
	ReadReplicas        []*client.Config `yaml:"read-replicas"`         // Read replica configs
}

// Node is the main struct for the node
type Node struct {
	Config             *Config              // Is the node configuration
	ConfigLock         *sync.RWMutex        // Is the lock for the config file
	Server             *server.Server       // Is the node server
	Logger             *slog.Logger         // Is the logger for the node
	ReplicaConnections []*ReplicaConnection // Are the connections to read replicas
	SharedKey          string               // Is the shared key for the node
	Storage            *hashtable.HashTable // Is the storage for the node
	Journal            *journal.Journal     // Is the journal for the node
	Lock               *sync.RWMutex        // Is the lock for the node
	MaxMemory          uint64               // Is the maximum memory for the system
	Wd                 string               // Is the working directory for the node
}

// ReplicaConnection is the connection to a read replica
type ReplicaConnection struct {
	Client  *client.Client  // Is the connection to the master node
	Health  bool            // Is the health status of the node
	Context context.Context // Is the context for the node
}

// ServerConnectionHandler is the handler for the server connections
type ServerConnectionHandler struct {
	Node        *Node // Node instance
	BufferSize  int   // Defined buffer size for the handler
	ReadTimeout int   // Defined read timeout for the handler
}

// New creates a new node instance
func New(logger *slog.Logger, sharedKey string) (*Node, error) {
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

	return &Node{Logger: logger, SharedKey: sharedKey, Storage: hashtable.New(), Lock: &sync.RWMutex{}, MaxMemory: maxMem, ConfigLock: &sync.RWMutex{}}, nil
}

// Open opens a new node instance
func (n *Node) Open(dir *string) error {

	var wd string
	var err error
	if dir == nil {
		// We get the current working directory
		wd, err = os.Getwd()
		if err != nil {
			return err
		}
	} else {
		wd = *dir

	}
	// Config for node
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

	// Set the node configuration
	n.Config = conf
	n.Wd = wd

	// We create a new server
	n.Server = server.New(n.Config.ServerConfig, n.Logger, &ServerConnectionHandler{
		Node:        n,
		BufferSize:  n.Config.ServerConfig.BufferSize,
		ReadTimeout: n.Config.ServerConfig.ReadTimeout,
	})

	// Create connections to the read replicas
	for _, replicaConfig := range n.Config.ReadReplicas {
		replicaConn := &ReplicaConnection{
			Client: client.New(replicaConfig, n.Logger),
			Health: false,
		}

		n.ReplicaConnections = append(n.ReplicaConnections, replicaConn)
	}

	n.Journal, err = journal.Open(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), JournalFile))
	if err != nil {
		return err
	}

	go n.backgroundHealthChecks()

	// We recover from journal
	// Populates the storage with the journal data
	if err = n.Journal.Recover(n.Storage); err != nil {
		return err
	}

	// We start the server
	err = n.Server.Start()
	if err != nil {
		return err
	}

	return nil
}

// Close closes the node instance gracefully
func (n *Node) Close() error {
	n.Lock.Lock()
	defer n.Lock.Unlock()

	// We close the server
	err := n.Server.Shutdown()
	if err != nil {
		return err
	}

	err = n.Journal.Close()
	if err != nil {
		return err
	}

	// Close all the replica connections
	for _, replicaConn := range n.ReplicaConnections {
		err = replicaConn.Client.Close()
		if err != nil {
			return err
		}

	}

	return nil
}

// openExistingConfigFile opens an existing node config file
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

// createDefaultConfigFile creates a default node config file
func createDefaultConfigFile(wd string) (*Config, error) {
	// We create the default node config file
	f, err := os.Create(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), ConfigFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	config := &Config{
		HealthCheckInterval: 2,
		MaxMemoryThreshold:  75,
		ServerConfig: &server.Config{
			Address:     "localhost:4001",
			UseTLS:      false,
			CertFile:    "/",
			KeyFile:     "/",
			ReadTimeout: 10,
			BufferSize:  1024,
		},
		ReadReplicas: []*client.Config{
			{
				ServerAddress:  "localhost:4002",
				UseTLS:         false,
				CACertFile:     "/",
				ConnectTimeout: 5,
				WriteTimeout:   5,
				ReadTimeout:    5,
				MaxRetries:     3,
				RetryWaitTime:  1,
				BufferSize:     1024,
			},
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
				h.Node.Logger.Warn("connection timeout", "remote_addr", conn.RemoteAddr())
			} else {
				h.Node.Logger.Warn("read error", "error", err, "remote_addr", conn.RemoteAddr())
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

		switch {
		case strings.HasPrefix(string(command), "NAUTH"):

			if !authenticated {

				sharedKey := strings.Split(string(command), " ")[1]

				// We hash the shared key
				sharedKeyHash := sha256.Sum256([]byte(h.Node.SharedKey))

				// We compare the shared key hash
				if fmt.Sprintf("%x", sharedKeyHash) == sharedKey {
					_, err = conn.Write([]byte("OK authenticated\r\n"))
					if err != nil {
						h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}

					authenticated = true
					continue
				} else {
					_, err = conn.Write([]byte("ERR invalid key\r\n"))
					if err != nil {
						h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}
				}
			} else {
				_, err = conn.Write([]byte("ERR already authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}

		case strings.HasPrefix(string(command), "PING"):
			_, err = conn.Write([]byte("OK PONG\r\n"))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "REGX"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// Check for optional offset and limit
			// Can be REGX <pattern> <offset> <limit>
			// or REGX <pattern>
			var offset, limit *int
			if len(strings.Split(string(command), " ")) > 2 {
				*offset, _ = strconv.Atoi(strings.Split(string(command), " ")[2])
				*limit, _ = strconv.Atoi(strings.Split(string(command), " ")[3])
			}

			pattern := strings.Split(string(command), " ")[1]

			var results [][]byte

			// We acquire read lock
			h.Node.Lock.RLock()
			entries, err := h.Node.Storage.GetWithRegex(pattern, offset, limit)
			if err != nil {
				_, err = conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.Node.Lock.RUnlock()
					return
				}
				h.Node.Lock.RUnlock()
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
			h.Node.Lock.RUnlock()
			// We join all results into a single byte slice
			_, err = conn.Write(bytes.Join(results, []byte("")))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "PUT"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			if h.Node.MemoryCheck() == false {
				// We are out of memory
				_, err = conn.Write([]byte("ERR out of memory\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We put the data
			key := strings.Split(string(command), " ")[1]
			value := strings.Join(strings.Split(string(command), " ")[2:], " ")

			// We lock the node
			h.Node.Lock.Lock()

			h.Node.Storage.Put(key, value)

			go func() {
				err := h.Node.Journal.Append(key, value, journal.PUT)
				if err != nil {
					h.Node.Logger.Warn("journal append error", "error", err)
				}
			}()

			// We relay to the read replicas
			h.Node.relayToReplicas(string(command))
			// We unlock the node
			h.Node.Lock.Unlock()

			_, err = conn.Write([]byte("OK key-value written\r\n"))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "GET"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We get the data
			key := strings.Split(string(command), " ")[1]

			// We get read lock
			h.Node.Lock.RLock()

			value, ts, ok := h.Node.Storage.Get(key)

			// We release read lock
			h.Node.Lock.RUnlock()

			if ok {
				// Format time in RFC3339
				// OK 2021-09-01T12:00:00Z key value
				_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, value)))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {
				_, err = conn.Write([]byte("ERR key not found\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "DEL"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We delete the data
			key := strings.Split(string(command), " ")[1]

			// We get lock
			h.Node.Lock.Lock()

			ok := h.Node.Storage.Delete(key)

			if ok {
				go func() {
					err := h.Node.Journal.Append(key, "", journal.DEL)
					if err != nil {
						h.Node.Logger.Warn("journal append error", "error", err)
					}
				}()

				// We release lock
				h.Node.Lock.Unlock()

				// We relay to the read replicas
				h.Node.relayToReplicas(string(command))

				_, err = conn.Write([]byte("OK key-value deleted\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {

				// We release lock
				h.Node.Lock.Unlock()

				_, err = conn.Write([]byte("ERR key-value not found\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "INCR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			key := strings.Split(string(command), " ")[1] // We get incrementing key

			// We check if we have incrementing value
			if len(strings.Split(string(command), " ")) < 3 {
				_, err := conn.Write([]byte("ERR invalid value\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				return
			}

			// We get lock
			h.Node.Lock.Lock()

			val, ts, err := h.Node.Storage.Incr(key, strings.Split(string(command), " ")[2])
			if err != nil {
				_, err := conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.Node.Lock.Unlock()
					return
				}
				h.Node.Lock.Unlock()
				return
			}
			go func() {
				err = h.Node.Journal.Append(key, strings.Split(string(command), " ")[2], journal.PUT)
				if err != nil {
					h.Node.Logger.Warn("journal append error", "error", err)
				}
			}()

			h.Node.Lock.Unlock()

			// We relay to the read replicas
			h.Node.relayToReplicas(string(command))

			_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, val)))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "DECR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			key := strings.Split(string(command), " ")[1] // We get decrementing key

			// We check if we have a decrementing value
			if len(strings.Split(string(command), " ")) < 3 {
				_, err := conn.Write([]byte("ERR invalid value\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				return
			}

			// We get lock
			h.Node.Lock.Lock()

			val, ts, err := h.Node.Storage.Decr(key, strings.Split(string(command), " ")[2])
			if err != nil {
				_, err := conn.Write([]byte(fmt.Sprintf("ERR %s\r\n", err.Error())))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					h.Node.Lock.Unlock()
					return
				}
				h.Node.Lock.Unlock()
				return
			}
			go func() {
				err = h.Node.Journal.Append(key, strings.Split(string(command), " ")[2], journal.PUT)
				if err != nil {
					h.Node.Logger.Warn("journal append error", "error", err)
				}
			}()

			h.Node.Lock.Unlock()

			// We relay to the read replicas
			h.Node.relayToReplicas(string(command))

			_, err = conn.Write([]byte(fmt.Sprintf("OK %s %s %s\r\n", ts.Format(time.RFC3339), key, val)))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "QUIT"):
			_, err = conn.Write([]byte("OK see ya later\r\n"))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

			return
		case strings.HasPrefix(string(command), "STAT"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			storageStats := h.Node.Journal.Pager.Stats()
			hashtableStats := h.Node.Storage.Stats()

			// We create one byte array for response
			var response []byte
			response = append(response, []byte("OK\r\n")...)
			response = append(response, []byte(fmt.Sprintf("DISK\r\n"))...)
			for k, v := range storageStats {
				response = append(response, []byte(fmt.Sprintf("\t%s %v\r\n", k, v))...)
			}

			response = append(response, []byte(fmt.Sprintf("MEMORY\r\n"))...)
			for k, v := range hashtableStats {
				response = append(response, []byte(fmt.Sprintf("\t%s %v\r\n", k, v))...)
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "RCNF"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			err = h.Node.ReloadConfig()
			if err != nil {
				_, err = conn.Write([]byte("ERR reload error\r\n"))
				if err != nil {
					h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}

			_, err = conn.Write([]byte("OK configs reloaded\r\n"))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		default:
			_, err = conn.Write([]byte("ERR unknown command\r\n"))
			if err != nil {
				h.Node.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		}

	}
}

// backgroundHealthChecks performs background health checks on the nodes
func (n *Node) backgroundHealthChecks() {
	ticker := time.NewTicker(time.Duration(n.Config.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			for _, replicaConn := range n.ReplicaConnections {
				n.Lock.RLock()
				if !replicaConn.Health {

					n.Logger.Warn("node replica is unhealthy", "replica", replicaConn.Client.Config.ServerAddress)
					if replicaConn.Context == nil {
						replicaConn.Context = context.Background()
					}
					if replicaConn.Client == nil {
						replicaConn.Client = client.New(replicaConn.Client.Config, n.Logger)

					}

					if err := replicaConn.Client.Connect(replicaConn.Context); err != nil {
						n.Logger.Warn("node connection error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					sharedKeyHash := sha256.Sum256([]byte(n.SharedKey))
					err := replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("NAUTH %x\r\n", sharedKeyHash)))
					if err != nil {
						n.Logger.Warn("authentication error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					response, err := replicaConn.Client.Receive(replicaConn.Context)
					if err != nil || string(response) != "OK authenticated\r\n" {
						n.Logger.Warn("authentication error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					replicaConn.Health = true
					n.Logger.Info("node replica is reconnected and healthy", "node", replicaConn.Client.Config.ServerAddress)

					err = replicaConn.Client.Send(replicaConn.Context, []byte("STARTSYNC\r\n"))
					if err != nil {
						n.Logger.Warn("sync error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					response, err = replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						n.Logger.Warn("read error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					command := strings.TrimSuffix(string(response), "\r\n")
					if !strings.HasPrefix(command, "SYNCFROM") {
						n.Lock.RUnlock()
						continue
					}

					parts := strings.Split(command, " ")
					if len(parts) < 2 {
						err = replicaConn.Client.Send(replicaConn.Context, []byte("ERR invalid command\r\n"))
						if err != nil {
							n.Logger.Warn("write error", "error", err, "remote_addr", replicaConn.Client.Conn.RemoteAddr())
						}
						n.Lock.RUnlock()
						continue
					}

					lastJournalPage := parts[1]
					lastJournalPageInt, err := strconv.Atoi(lastJournalPage)
					if err != nil {
						n.Logger.Warn("invalid page number", "error", err)
						n.Lock.RUnlock()
						continue
					}

					it, err := pager.NewIteratorAtPage(n.Journal.Pager, lastJournalPageInt)
					if err != nil {
						if err.Error() == "invalid start page: must be >= 0" || err.Error() == "start page exceeds maximum pages 0" {
							err = replicaConn.Client.Send(replicaConn.Context, []byte("DONESYNC\r\n"))
							if err != nil {
								n.Logger.Warn("write error", "error", err, "remote_addr", replicaConn.Client.Conn.RemoteAddr())
							}
							n.Logger.Warn("nothing to sync", "remote_addr", replicaConn.Client.Conn.RemoteAddr())
							n.Lock.RUnlock()
							continue
						}
						err = replicaConn.Client.Send(replicaConn.Context, []byte("ERR invalid command\r\n"))
						if err != nil {
							n.Logger.Warn("write error", "error", err, "remote_addr", replicaConn.Client.Conn.RemoteAddr())
						}
						n.Lock.RUnlock()
						continue
					}

					for it.Next() {
						data, err := it.Read()
						if err != nil {
							break
						}

						e, err := journal.Deserialize(data)
						if err != nil {
							continue
						}

						switch e.Op {
						case journal.PUT:
							err = replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("PUT %s %s\r\n", e.Key, e.Value)))
						case journal.DEL:
							err = replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("DEL %s\r\n", e.Key)))
						case journal.INCR:
							err = replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("INCR %s %s\r\n", e.Key, e.Value)))
						case journal.DECR:
							err = replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("DECR %s %s\r\n", e.Key, e.Value)))

						}
						if err != nil {
							n.Logger.Warn("write error", "error", err, "remote_addr", replicaConn.Client.Conn.RemoteAddr())
							break
						}

						// Read response
						response, err = replicaConn.Client.Receive(replicaConn.Context)
						if err != nil {
							n.Logger.Warn("read error", "error", err)
							break
						}

						if strings.HasPrefix(string(response), "ERR") {
							n.Logger.Warn("unexpected response", "response", string(response))
							break
						}

					}

					err = replicaConn.Client.Send(replicaConn.Context, []byte("SYNCDONE\r\n"))
					if err != nil {
						n.Logger.Warn("write error", "error", err, "remote_addr", replicaConn.Client.Conn.RemoteAddr())
					}

					// Read response
					response, err = replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						n.Logger.Warn("read error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					if string(response) != "OK synced\r\n" {
						n.Logger.Warn("unexpected response", "response", string(response))
						n.Lock.RUnlock()
						continue
					}

				} else {
					// We create a temp connection to the replica using a new client
					tempClient := client.New(replicaConn.Client.Config, n.Logger)
					if err := tempClient.Connect(replicaConn.Context); err != nil {
						n.Logger.Warn("node connection error", "error", err)
						n.Lock.RUnlock()
						continue
					}

					err := tempClient.Send(replicaConn.Context, []byte("PING\r\n"))
					if err != nil {
						replicaConn.Health = false
						tempClient.Close()
						n.Lock.RUnlock()
						continue
					}

					response, err := tempClient.Receive(replicaConn.Context)
					if err != nil {
						n.Logger.Warn("read error", "error", err)
						replicaConn.Health = false
						tempClient.Close()
						n.Lock.RUnlock()
						continue
					}

					if string(response) != "OK PONG\r\n" {
						n.Logger.Warn("unexpected response", "response", string(response))
						replicaConn.Health = false
					}

					tempClient.Close()
					n.Lock.RUnlock()

				}

			}
		}
	}
}

// relayToReplicas relays the command to the read replicas
func (n *Node) relayToReplicas(command string) {
	for _, replicaConn := range n.ReplicaConnections {
		if replicaConn.Health {
			// We send the command to the replica
			err := replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("%s\r\n", command)))
			if err != nil {
				n.Logger.Warn("write error", "error", err)
				replicaConn.Health = false

				continue
			}

			// We read the response
			_, err = replicaConn.Client.Receive(replicaConn.Context)
			if err != nil {
				n.Logger.Warn("read error", "error", err)
				replicaConn.Health = false

				continue
			}

		}

	}
}

// MemoryCheck checks the memory usage of the node
// true for ok (not out of memory), false for out of memory
func (n *Node) MemoryCheck() bool {
	// Get the current memory usage
	currentMemoryUsage := utility.GetCurrentMemoryUsage()

	// Calculate the percentage of current memory usage relative to nr.MaxMemory
	memoryUsagePercentage := (float64(currentMemoryUsage) / float64(n.MaxMemory)) * 100

	// We compare it with the MaxMemoryThreshold set in Config
	if memoryUsagePercentage > float64(n.Config.MaxMemoryThreshold) {
		return false
	}

	return true
}

// ReloadConfig reloads node config file
func (n *Node) ReloadConfig() error {
	n.ConfigLock.Lock()
	defer n.ConfigLock.Unlock()
	// We open the existing config file
	config, err := openExistingConfigFile(n.Wd)
	if err != nil {
		return err
	}

	// We update the node config
	n.Config = config

	// We update the server config
	n.Server.Config = config.ServerConfig

	for _, replicaConfig := range config.ReadReplicas {
		// We check if the replica connection already exists
		var exists bool
		for _, replicaConn := range n.ReplicaConnections {
			if replicaConn.Client.Config.ServerAddress == replicaConfig.ServerAddress {
				exists = true
				break
			}
		}

		if exists {
			continue
		}

		replicaConn := &ReplicaConnection{
			Client: client.New(replicaConfig, n.Logger),
			Health: false,
		}

		n.ReplicaConnections = append(n.ReplicaConnections, replicaConn)
	}

	// Now we find what replicas to remove
	var remove []int
	for i, replicaConn := range n.ReplicaConnections {
		var found bool
		for _, replicaConfig := range config.ReadReplicas {
			if replicaConfig.ServerAddress == replicaConn.Client.Config.ServerAddress {
				found = true
				break
			}
		}

		if !found {
			remove = append(remove, i)
		}
	}

	// We remove the replicas
	for _, i := range remove {
		// We close the client
		if n.ReplicaConnections[i].Client != nil {
			err = n.ReplicaConnections[i].Client.Close()
			if err != nil {
				return err
			}
		}
		n.ReplicaConnections = append(n.ReplicaConnections[:i], n.ReplicaConnections[i+1:]...)
	}

	return nil
}
