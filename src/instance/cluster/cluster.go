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
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
	"supermassive/network/client"
	"supermassive/network/server"
	"sync"
	"sync/atomic"
	"time"
)

// ConfigFile is cluster config name
const ConfigFile = ".cluster"

// The cluster runs a server and has many client connections to nodes and their read replicas.

// Config is the cluster configurations
type Config struct {
	HealthCheckInterval int            `yaml:"health-check-interval"` // Health check interval
	ServerConfig        *server.Config `yaml:"server-config"`         // Cluster server configs
	NodeConfigs         []*NodeConfig  `yaml:"node-configs"`          // Node configurations
}

// NodeConfig is the configuration for a node within cluster
type NodeConfig struct {
	Node     *client.Config   // Node server configs
	Replicas []*client.Config // Read replica configs
}

// Cluster is the main struct for the cluster
type Cluster struct {
	Config              *Config           // Is the cluster configuration
	ConfigLock          *sync.RWMutex     // Is the config lock
	Server              *server.Server    // Is the cluster server
	NodeConnections     []*NodeConnection // Are the connections to nodes
	NodeConnectionsLock *sync.RWMutex     // Is the node connections lock
	Logger              *slog.Logger      // Is the logger for the cluster
	SharedKey           string            // Is the shared key for the cluster
	Sequence            atomic.Int32      // Is the sequence for writes to primary nodes
	Username            string            // Is the cluster user username to access through client
	Password            string            // Is the cluster user password to access through client
	Wd                  string            // Is the working directory
	Lock                *sync.RWMutex     // Is the lock for the cluster
}

// NodeConnection is the connection to a node
type NodeConnection struct {
	Client   *client.Client       // Is the connection to the master node
	Replicas []*ReplicaConnection // Are the connections to the read replicas
	Health   bool                 // Is the health status of the node
	Context  context.Context      // Is the context for the node
	Config   *NodeConfig          // Is the node configuration
	Lock     *sync.Mutex          // Is the lock for the node connection
}

// ReplicaConnection is a connection to a nodes read replica
type ReplicaConnection struct {
	Client  *client.Client  // Is the connection to the read replica
	Health  bool            // Is the health status of the read replica
	Context context.Context // Is the context for the read replica
	Config  *client.Config  // Is the read replica configuration
	Lock    *sync.Mutex     // Is the lock for the read replica connection
}

// ServerConnectionHandler is the handler for the server connections
type ServerConnectionHandler struct {
	Cluster     *Cluster // Cluster instance
	BufferSize  int      // Defined buffer size for the handler
	ReadTimeout int      // Defined read timeout for the handler
}

// New creates a new cluster instance
func New(logger *slog.Logger, sharedKey, username, password string) (*Cluster, error) {
	if sharedKey == "" {
		return nil, fmt.Errorf("shared key is required")
	}

	if username == "" {
		return nil, fmt.Errorf("username is required")
	}

	if password == "" {
		return nil, fmt.Errorf("password is required")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	return &Cluster{Logger: logger, SharedKey: sharedKey, Username: username, Password: password, Sequence: atomic.Int32{}, ConfigLock: &sync.RWMutex{}, NodeConnectionsLock: &sync.RWMutex{}, Lock: &sync.RWMutex{}}, nil
}

// Open opens a new cluster instance
func (c *Cluster) Open() error {
	// We get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Config for cluster
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

	// Set the cluster configuration
	c.Config = conf

	c.Wd = wd

	// We create a new server
	c.Server = server.New(c.Config.ServerConfig, c.Logger, &ServerConnectionHandler{
		Cluster:     c,
		BufferSize:  c.Config.ServerConfig.BufferSize,
		ReadTimeout: c.Config.ServerConfig.ReadTimeout,
	})

	// Create node connections for each node
	for _, nodeConfig := range c.Config.NodeConfigs {
		nodeConn := &NodeConnection{
			Config: nodeConfig,
			Lock:   &sync.Mutex{},
		}

		for _, replicaConfig := range nodeConfig.Replicas {
			replicaConn := &ReplicaConnection{
				Config: replicaConfig,
				Lock:   &sync.Mutex{},
			}
			nodeConn.Replicas = append(nodeConn.Replicas, replicaConn)
		}

		// Append the node connection to the cluster
		c.NodeConnections = append(c.NodeConnections, nodeConn)

	}

	go c.backgroundHealthChecks()

	// We start the server
	err = c.Server.Start()
	if err != nil {
		return err
	}

	return nil
}

// backgroundHealthChecks performs background health checks on the nodes
func (c *Cluster) backgroundHealthChecks() {
	ticker := time.NewTicker(time.Duration(c.Config.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, nodeConn := range c.NodeConnections {
				// We read lock the node connection
				nodeConn.Lock.Lock()

				if !nodeConn.Health {
					c.Logger.Warn("node is unhealthy", "node", nodeConn.Config.Node.ServerAddress)
					if nodeConn.Context == nil {
						nodeConn.Context = context.Background()
					}
					if nodeConn.Client == nil {
						nodeConn.Client = client.New(nodeConn.Config.Node, c.Logger)
					}

					if err := nodeConn.Client.Connect(nodeConn.Context); err != nil {
						c.Logger.Warn("node connection error", "error", err)
					} else {

						sharedKeyHash := sha256.Sum256([]byte(c.SharedKey))

						err := nodeConn.Client.Send(nodeConn.Context, []byte(fmt.Sprintf("NAUTH %x\r\n", sharedKeyHash)))
						if err != nil {
							c.Logger.Warn("authentication error", "error", err)
						} else {
							response, err := nodeConn.Client.Receive(nodeConn.Context)
							log.Println(string(response))
							if err != nil || string(response) != "OK authenticated\r\n" {
								c.Logger.Warn("authentication error", "error", err)
							} else {
								nodeConn.Health = true
								c.Logger.Info("node is reconnected and healthy", "node", nodeConn.Config.Node.ServerAddress)
							}
						}
					}
				} else {
					// We create a temp connection to the replica using a new client
					tempClient := client.New(nodeConn.Client.Config, c.Logger)
					if err := tempClient.Connect(nodeConn.Context); err != nil {
						c.Logger.Warn("node connection error", "error", err)
					} else {

						err := tempClient.Send(nodeConn.Context, []byte("PING\r\n"))
						if err != nil {
							nodeConn.Health = false
							tempClient.Close()
						} else {

							response, err := tempClient.Receive(nodeConn.Context)
							if err != nil {
								c.Logger.Warn("read error", "error", err)
								nodeConn.Health = false
							} else {

								if string(response) != "OK PONG\r\n" {
									c.Logger.Warn("unexpected response", "response", string(response))
									nodeConn.Health = false
								}
							}

							tempClient.Close()
						}
					}

				}

				for _, replicaConn := range nodeConn.Replicas {
					// We acquire the lock for the replica connection
					replicaConn.Lock.Lock()

					if !replicaConn.Health {
						c.Logger.Warn("replica is unhealthy", "replica", replicaConn.Config.ServerAddress)
						if replicaConn.Context == nil {
							replicaConn.Context = context.Background()
						}
						if replicaConn.Client == nil {
							replicaConn.Client = client.New(replicaConn.Config, c.Logger)
						}

						if err := replicaConn.Client.Connect(replicaConn.Context); err != nil {
							c.Logger.Warn("replica connection error", "error", err)
						} else {
							sharedKeyHash := sha256.Sum256([]byte(c.SharedKey))
							err := replicaConn.Client.Send(replicaConn.Context, []byte(fmt.Sprintf("NAUTH %x\r\n", sharedKeyHash)))
							if err != nil {
								c.Logger.Warn("replica authentication error", "error", err)
								// Unlock the replica connection
								replicaConn.Lock.Unlock()
								continue
							}
							response, err := replicaConn.Client.Receive(replicaConn.Context)
							if err != nil || string(response) != "OK authenticated\r\n" {
								c.Logger.Warn("replica authentication error", "error", err)
								// Unlock the replica connection
								replicaConn.Lock.Unlock()
								continue
							}
							replicaConn.Health = true
							c.Logger.Info("replica is reconnected and healthy", "replica", replicaConn.Config.ServerAddress)
						}

						// Unlock the replica connection
						replicaConn.Lock.Unlock()
					} else {
						// We create a temp connection to the replica using a new client
						tempClient := client.New(replicaConn.Client.Config, c.Logger)
						if err := tempClient.Connect(replicaConn.Context); err != nil {
							c.Logger.Warn("node connection error", "error", err)
							replicaConn.Health = false
							replicaConn.Lock.Unlock()
							continue
						}

						err := tempClient.Send(replicaConn.Context, []byte("PING\r\n"))
						if err != nil {
							replicaConn.Health = false
							tempClient.Close()
							replicaConn.Lock.Unlock()
							continue
						}

						response, err := tempClient.Receive(replicaConn.Context)
						if err != nil {
							c.Logger.Warn("read error", "error", err)
							replicaConn.Health = false
							tempClient.Close()
							replicaConn.Lock.Unlock()
							continue
						}

						if string(response) != "OK PONG\r\n" {
							c.Logger.Warn("unexpected response", "response", string(response))
							replicaConn.Health = false
						}

						tempClient.Close()

						replicaConn.Lock.Unlock()

					}
				}

				// Unlock the node connection
				nodeConn.Lock.Unlock()
			}
		}
	}
}

// Close closes the cluster instance gracefully
func (c *Cluster) Close() error {
	// We close the server
	err := c.Server.Shutdown()
	if err != nil {
		return err
	}

	// Close all the node connections
	for _, nodeConn := range c.NodeConnections {
		if nodeConn.Client != nil {
			if nodeConn.Health {
				err = nodeConn.Client.Close()
				if err != nil {
					return err
				}

				// Check for read replicas
				for _, replica := range nodeConn.Replicas {
					if replica.Client != nil {
						if replica.Health {
							err = replica.Client.Close()
							if err != nil {
								return err
							}
						}
					}

				}
			}

		}
	}

	return nil
}

// openExistingConfigFile opens an existing cluster config file
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

// createDefaultConfigFile creates a default cluster config file
func createDefaultConfigFile(wd string) (*Config, error) {
	// We create the default cluster config file
	f, err := os.Create(fmt.Sprintf("%s%s%s", wd, string(os.PathSeparator), ConfigFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	config := &Config{
		HealthCheckInterval: 2,
		ServerConfig: &server.Config{
			Address:     "localhost:4000",
			UseTLS:      false,
			CertFile:    "/",
			KeyFile:     "/",
			ReadTimeout: 10,
			BufferSize:  1024,
		},
		NodeConfigs: []*NodeConfig{
			{
				Node: &client.Config{

					ServerAddress:  "localhost:4001",
					UseTLS:         false,
					ConnectTimeout: 5,
					WriteTimeout:   5,
					ReadTimeout:    5,
					MaxRetries:     3,
					RetryWaitTime:  1,
					BufferSize:     1024,
				},
				Replicas: []*client.Config{
					{
						ServerAddress:  "localhost:4002",
						UseTLS:         false,
						ConnectTimeout: 5,
						WriteTimeout:   5,
						ReadTimeout:    5,
						MaxRetries:     3,
						RetryWaitTime:  1,
						BufferSize:     1024,
					},
				},
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

	authenticated := false // Whether client is authenticated to the cluster

	for {
		_ = conn.SetReadDeadline(time.Time{}) // Disable read deadline
		n, err := conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				h.Cluster.Logger.Warn("connection timeout", "remote_addr", conn.RemoteAddr())
			} else {
				h.Cluster.Logger.Warn("read error", "error", err, "remote_addr", conn.RemoteAddr())
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

		switch {
		case strings.HasPrefix(string(command), "AUTH"):
			// We check if the client is already authenticated
			if authenticated {
				_, err = conn.Write([]byte("ERR already authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We get base64 encoded "username\0password"
			credentials := bytes.TrimSuffix(bytes.Split(command, []byte(" "))[1], []byte("\r\n"))

			decoded, err := base64.StdEncoding.DecodeString(string(credentials))
			if err != nil {
				_, err = conn.Write([]byte("ERR invalid credentials\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We split the decoded credentials into username and password
			creds := bytes.Split(decoded, []byte("\\0"))
			if len(creds) != 2 {
				_, err = conn.Write([]byte("ERR invalid credentials\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if the username and password match
			if string(creds[0]) != h.Cluster.Username || string(creds[1]) != h.Cluster.Password {
				_, err = conn.Write([]byte("ERR invalid credentials\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We authenticate the client
			authenticated = true
			_, err = conn.Write([]byte("OK authenticated\r\n"))
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
			continue
		case strings.HasPrefix(string(command), "PING"):
			_, err = conn.Write([]byte("OK PONG\r\n"))
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "PUT"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			response, err := h.Cluster.WriteToNode(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR write error\r\n"))
				if err != nil {
					h.Cluster.NodeConnectionsLock.RUnlock()
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write(response)
				if err != nil {
					h.Cluster.NodeConnectionsLock.RUnlock()
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}
		case strings.HasPrefix(string(command), "DEL"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			response, err := h.Cluster.ParallelDelete(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR read error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "GET"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			response, err := h.Cluster.ParallelGet(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR read error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}

				if err != nil {
					_, err = conn.Write([]byte("ERR write error\r\n"))
					if err != nil {
						h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}
				}

				continue
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "REGX"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			response, err := h.Cluster.ParallelRegx(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR read error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}

				if err != nil {
					_, err = conn.Write([]byte("ERR write error\r\n"))
					if err != nil {
						h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}
				}

				continue
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "INCR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We write to all primary nodes in parallel
			response, err := h.Cluster.ParallelIncrDecr(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR write error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}

				if err != nil {
					_, err = conn.Write([]byte("ERR write error\r\n"))
					if err != nil {
						h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
						return
					}
				}

				continue
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "DECR"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We check if there are any primary nodes
			h.Cluster.NodeConnectionsLock.RLock()
			if len(h.Cluster.NodeConnections) == 0 {
				h.Cluster.NodeConnectionsLock.RUnlock()
				_, err = conn.Write([]byte("ERR no primary nodes available\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			// We write to all primary nodes in parallel
			response, err := h.Cluster.ParallelIncrDecr(command)
			if err != nil {
				_, err = conn.Write([]byte("ERR write error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}

			_, err = conn.Write(response)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

		case strings.HasPrefix(string(command), "QUIT"):
			_, err = conn.Write([]byte("OK see ya later\r\n"))
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}

			return
		case strings.HasPrefix(string(command), "STAT"):
			stats := h.Cluster.Stats()

			_, err = conn.Write(stats)
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		case strings.HasPrefix(string(command), "RCNF"):
			if !authenticated {
				_, err = conn.Write([]byte("ERR not authenticated\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
				continue
			}

			err = h.Cluster.ReloadConfig()
			if err != nil {
				_, err = conn.Write([]byte("ERR reload error\r\n"))
				if err != nil {
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			}

			_, err = conn.Write([]byte("OK configs reloaded\r\n"))
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		default:
			_, err = conn.Write([]byte("ERR unknown command\r\n"))
			if err != nil {
				h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
				return
			}
		}

	}
}

// clusterStats retrieves basic stats for the cluster
func (c *Cluster) clusterStats() []byte {
	response := []byte(fmt.Sprintf("CLUSTER %s\r\n", c.Config.ServerConfig.Address))

	// current sequence n
	response = append(response, fmt.Sprintf("\tcurrent_sequence %d\r\n", c.Sequence.Load())...)
	response = append(response, fmt.Sprintf("\tclient_connection_count %d\r\n", c.Server.GetConnCount())...)

	return response
}

// ParallelDelete deletes a key from all primary nodes in parallel
func (c *Cluster) ParallelDelete(command []byte) ([]byte, error) {
	// We delete from all primary nodes
	var response *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}

	responseChannel := make(chan *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}, len(c.NodeConnections))

	wg := sync.WaitGroup{}

	for _, nodeConn := range c.NodeConnections {
		nodeConn.Lock.Lock()

		if !nodeConn.Health {
			nodeConn.Lock.Unlock()
			continue
		}

		wg.Add(1)
		go func(nodeConn *NodeConnection) {
			defer wg.Done()
			defer nodeConn.Lock.Unlock() // Always release the lock

			// We send the command
			err := nodeConn.Client.Send(nodeConn.Context, command)
			if err != nil {
				c.Logger.Warn("write error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
				return
			}

			// We receive the response
			rec, err := nodeConn.Client.Receive(nodeConn.Context)
			if err != nil {
				c.Logger.Warn("read error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
				return
			}

			// If OK
			if bytes.HasPrefix(rec, []byte("OK")) {
				parts := bytes.Split(rec, []byte(" "))
				if len(parts) < 3 {
					c.Logger.Warn("invalid response format", "response", string(rec), "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// We parse the received response
				timestamp := parts[1]
				data := bytes.Join(parts[2:], []byte(" "))

				ts, err := time.Parse(time.RFC3339, string(timestamp))
				if err != nil {
					c.Logger.Warn("time parse error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// Send the response to the channel
				responseChannel <- &struct {
					TimeStamp time.Time
					Data      []byte
					Node      *NodeConnection
				}{
					TimeStamp: ts,
					Data:      data,
					Node:      nodeConn,
				}
			} else {
				// This node doesn't have the key - log but don't treat as error
				c.Logger.Debug("node doesn't have key to delete",
					"node", nodeConn.Config.Node.ServerAddress,
					"response", string(rec),
					"command", string(command))
			}
		}(nodeConn)
	}

	// Wait in a separate goroutine and close the channel when done
	go func() {
		wg.Wait()
		close(responseChannel)
	}()

	// Process all responses and find the one with the most recent timestamp
	responseLock := sync.Mutex{}
	for resp := range responseChannel {
		responseLock.Lock()
		if response == nil || resp.TimeStamp.After(response.TimeStamp) {
			response = resp
		}
		responseLock.Unlock()
	}

	// If we didn't get any successful responses
	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	// Clean up any older versions on other nodes
	for _, nodeConn := range c.NodeConnections {
		if nodeConn != response.Node && nodeConn.Health {
			// Run deletion in background to not block the main flow
			go func(node *NodeConnection) {
				node.Lock.Lock()
				defer node.Lock.Unlock()

				key := bytes.Split(command, []byte(" "))[1]
				err := node.Client.Send(node.Context, []byte(fmt.Sprintf("DEL %s", key)))
				if err != nil {
					c.Logger.Warn("cleanup delete error", "error", err, "node", node.Config.Node.ServerAddress)
					return
				}

				// We don't need to check the response - it might not exist on this node
				_, _ = node.Client.Receive(node.Context)
			}(nodeConn)
		}
	}

	return response.Data, nil
}

// Stats get stats on the cluster and it's nodes
func (c *Cluster) Stats() []byte {
	response := []byte("OK\r\n")

	// We get the cluster stats
	response = append(response, c.clusterStats()...)

	// We send a stat command to all nodes in parallel to avoid blocking
	var wg sync.WaitGroup
	var responseLock sync.Mutex

	for _, nodeConn := range c.NodeConnections {
		nodeConn.Lock.Lock()

		// Add node header to response
		responseLock.Lock()
		response = append(response, fmt.Sprintf("PRIMARY %s\r\n", nodeConn.Config.Node.ServerAddress)...)
		responseLock.Unlock()

		if !nodeConn.Health {
			// Node is down
			responseLock.Lock()
			response = append(response, []byte("down\r\n")...)
			responseLock.Unlock()
			nodeConn.Lock.Unlock()
		} else {
			// Use goroutine to get stats from healthy node
			wg.Add(1)
			go func(nc *NodeConnection) {
				defer wg.Done()
				defer nc.Lock.Unlock()

				// Send the STAT command
				if err := nc.Client.Send(nc.Context, []byte("STAT\r\n")); err != nil {
					c.Logger.Warn("write error", "error", err, "node", nc.Config.Node.ServerAddress)
					responseLock.Lock()
					response = append(response, []byte("error: failed to send command\r\n")...)
					responseLock.Unlock()
					return
				}

				// Receive the response
				rec, err := nc.Client.Receive(nc.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err, "node", nc.Config.Node.ServerAddress)
					responseLock.Lock()
					response = append(response, []byte("error: failed to receive stats\r\n")...)
					responseLock.Unlock()
					return
				}

				// Trim the OK prefix and add to response
				rec = bytes.TrimPrefix(rec, []byte("OK\r\n"))
				responseLock.Lock()
				response = append(response, rec...)
				responseLock.Unlock()
			}(nodeConn)
		}

		// Process replicas for this node
		for _, replicaConn := range nodeConn.Replicas {
			replicaConn.Lock.Lock()

			// Add replica header to response
			responseLock.Lock()
			response = append(response, fmt.Sprintf("REPLICA %s\r\n", replicaConn.Config.ServerAddress)...)
			responseLock.Unlock()

			if !replicaConn.Health {
				// Replica is down
				responseLock.Lock()
				response = append(response, []byte("down\r\n")...)
				responseLock.Unlock()
				replicaConn.Lock.Unlock()
			} else {
				// Use goroutine to get stats from healthy replica
				wg.Add(1)
				go func(rc *ReplicaConnection) {
					defer wg.Done()
					defer rc.Lock.Unlock()

					// Send the STAT command
					if err := rc.Client.Send(rc.Context, []byte("STAT\r\n")); err != nil {
						c.Logger.Warn("write error", "error", err, "replica", rc.Config.ServerAddress)
						responseLock.Lock()
						response = append(response, []byte("error: failed to send command\r\n")...)
						responseLock.Unlock()
						return
					}

					// Receive the response
					rec, err := rc.Client.Receive(rc.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err, "replica", rc.Config.ServerAddress)
						responseLock.Lock()
						response = append(response, []byte("error: failed to receive stats\r\n")...)
						responseLock.Unlock()
						return
					}

					// Trim the OK prefix and add to response
					rec = bytes.TrimPrefix(rec, []byte("OK\r\n"))
					responseLock.Lock()
					response = append(response, rec...)
					responseLock.Unlock()
				}(replicaConn)
			}
		}
	}

	// Wait for all stats gathering to complete
	wg.Wait()

	return response
}

// ParallelIncrDecr increments a key in all primary nodes in parallel
func (c *Cluster) ParallelIncrDecr(command []byte) ([]byte, error) {
	// We incr|decr from all primary nodes
	var response *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}

	responseChannel := make(chan *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}, len(c.NodeConnections))

	wg := sync.WaitGroup{}

	for _, nodeConn := range c.NodeConnections {
		nodeConn.Lock.Lock()

		if !nodeConn.Health {
			nodeConn.Lock.Unlock()
			continue
		}

		wg.Add(1)
		go func(nodeConn *NodeConnection) {
			defer wg.Done()
			defer nodeConn.Lock.Unlock() // Always release the lock

			// We send the command
			err := nodeConn.Client.Send(nodeConn.Context, command)
			if err != nil {
				c.Logger.Warn("write error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
				return
			}

			// We receive the response
			rec, err := nodeConn.Client.Receive(nodeConn.Context)
			if err != nil {
				c.Logger.Warn("read error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
				return
			}

			// If OK
			if bytes.HasPrefix(rec, []byte("OK")) {
				parts := bytes.Split(rec, []byte(" "))
				if len(parts) < 3 {
					c.Logger.Warn("invalid response format", "response", string(rec), "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// We parse the received response
				timestamp := parts[1]
				data := bytes.Join(parts[2:], []byte(" "))

				ts, err := time.Parse(time.RFC3339, string(timestamp))
				if err != nil {
					c.Logger.Warn("time parse error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// Send the response to the channel
				responseChannel <- &struct {
					TimeStamp time.Time
					Data      []byte
					Node      *NodeConnection
				}{
					TimeStamp: ts,
					Data:      data,
					Node:      nodeConn,
				}
			} else {
				// This node doesn't have the key or had an error - log but don't treat as error
				c.Logger.Debug("node operation failed for incr/decr",
					"node", nodeConn.Config.Node.ServerAddress,
					"response", string(rec),
					"command", string(command))
			}
		}(nodeConn)
	}

	// Wait in a separate goroutine and close the channel when done
	go func() {
		wg.Wait()
		close(responseChannel)
	}()

	// Process all responses and find the one with the most recent timestamp
	responseLock := sync.Mutex{}
	for resp := range responseChannel {
		responseLock.Lock()
		if response == nil || resp.TimeStamp.After(response.TimeStamp) {
			// If we found a newer response, clean up the older one
			if response != nil {
				// Run deletion in background to not block the main flow
				go func(oldNode *NodeConnection) {
					oldNode.Lock.Lock()
					defer oldNode.Lock.Unlock()

					if !oldNode.Health {
						return
					}

					key := bytes.Split(command, []byte(" "))[1]
					err := oldNode.Client.Send(oldNode.Context, []byte(fmt.Sprintf("DEL %s", key)))
					if err != nil {
						c.Logger.Warn("cleanup delete error", "error", err, "node", oldNode.Config.Node.ServerAddress)
						return
					}

					// Check delete response
					delResp, err := oldNode.Client.Receive(oldNode.Context)
					if err != nil {
						c.Logger.Warn("cleanup read error", "error", err, "node", oldNode.Config.Node.ServerAddress)
						return
					}

					if bytes.HasPrefix(delResp, []byte("OK")) {
						c.Logger.Info("deleted stale key during incr/decr",
							"key", string(bytes.Split(command, []byte(" "))[1]),
							"node", oldNode.Config.Node.ServerAddress)
					}
				}(response.Node)
			}

			response = resp
		}
		responseLock.Unlock()
	}

	// If we didn't get any successful responses
	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	return response.Data, nil
}

// ParallelGet reads from all replicas in parallel
func (c *Cluster) ParallelGet(command []byte) ([]byte, error) {
	// We get from all primary nodes and replicas
	var response *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}

	responseChannel := make(chan *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}, len(c.NodeConnections)*2) // Buffer for all possible responses

	wg := sync.WaitGroup{}
	responseLock := sync.Mutex{}

	// Process all node connections
	for _, nodeConn := range c.NodeConnections {
		nodeConn.Lock.Lock()

		// Check if the primary node is healthy
		if nodeConn.Health {
			// Handle primary node
			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()
				defer nodeConn.Lock.Unlock() // Always release the lock

				// Send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// Receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// If the response starts with "OK", this node has the data
				if bytes.HasPrefix(rec, []byte("OK")) {
					parts := bytes.Split(rec, []byte(" "))
					if len(parts) < 3 {
						c.Logger.Warn("invalid response format", "response", string(rec), "node", nodeConn.Config.Node.ServerAddress)
						return
					}

					timestamp := parts[1]
					data := bytes.Join(parts[2:], []byte(" "))

					ts, err := time.Parse(time.RFC3339, string(timestamp))
					if err != nil {
						c.Logger.Warn("time parse error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
						return
					}

					// Send this response to the channel
					responseChannel <- &struct {
						TimeStamp time.Time
						Data      []byte
						Node      *NodeConnection
					}{
						TimeStamp: ts,
						Data:      data,
						Node:      nodeConn,
					}
				} else {
					// This node doesn't have the data - log it but don't treat as an error
					c.Logger.Debug("node doesn't have key",
						"node", nodeConn.Config.Node.ServerAddress,
						"response", string(rec),
						"command", string(command))
				}
			}(nodeConn)
		} else {
			// Primary node is unhealthy, release its lock
			nodeConn.Lock.Unlock()

			// Try replicas for this node
			for _, replicaConn := range nodeConn.Replicas {
				replicaConn.Lock.Lock()

				if !replicaConn.Health {
					// Replica is unhealthy, skip it
					replicaConn.Lock.Unlock()
					continue
				}

				// Process healthy replica
				wg.Add(1)
				go func(nodeConn *NodeConnection, replicaConn *ReplicaConnection) {
					defer wg.Done()
					defer replicaConn.Lock.Unlock() // Always release the lock

					// Send the command
					err := replicaConn.Client.Send(replicaConn.Context, command)
					if err != nil {
						c.Logger.Warn("write error", "error", err, "replica", replicaConn.Config.ServerAddress)
						return
					}

					// Receive the response
					rec, err := replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err, "replica", replicaConn.Config.ServerAddress)
						return
					}

					// If the response starts with "OK", this replica has the data
					if bytes.HasPrefix(rec, []byte("OK")) {
						parts := bytes.Split(rec, []byte(" "))
						if len(parts) < 3 {
							c.Logger.Warn("invalid response format", "response", string(rec), "replica", replicaConn.Config.ServerAddress)
							return
						}

						timestamp := parts[1]
						data := bytes.Join(parts[2:], []byte(" "))

						ts, err := time.Parse(time.RFC3339, string(timestamp))
						if err != nil {
							c.Logger.Warn("time parse error", "error", err, "replica", replicaConn.Config.ServerAddress)
							return
						}

						// Send this response to the channel
						responseChannel <- &struct {
							TimeStamp time.Time
							Data      []byte
							Node      *NodeConnection
						}{
							TimeStamp: ts,
							Data:      data,
							Node:      nodeConn,
						}
					} else {
						// This replica doesn't have the data - log it but don't treat as an error
						c.Logger.Debug("replica doesn't have key",
							"replica", replicaConn.Config.ServerAddress,
							"response", string(rec),
							"command", string(command))
					}
				}(nodeConn, replicaConn)
			}
		}
	}

	// Wait in a separate goroutine
	go func() {
		wg.Wait()
		close(responseChannel)
	}()

	// Process all responses and find the one with the most recent timestamp
	for resp := range responseChannel {
		responseLock.Lock()
		if response == nil || resp.TimeStamp.After(response.TimeStamp) {
			// If we already have a response with an older timestamp, we should delete that key
			if response != nil {
				key := bytes.Split(command, []byte(" "))[1]

				// Run deletion in background to not block the main flow
				go func(oldNode *NodeConnection, keyToDelete []byte) {
					oldNode.Lock.Lock()
					defer oldNode.Lock.Unlock()

					if !oldNode.Health {
						return
					}

					err := oldNode.Client.Send(oldNode.Context, []byte(fmt.Sprintf("DEL %s", keyToDelete)))
					if err != nil {
						c.Logger.Warn("write error during cleanup", "error", err, "node", oldNode.Config.Node.ServerAddress)
						return
					}

					// Check deletion response
					delResp, err := oldNode.Client.Receive(oldNode.Context)
					if err != nil {
						c.Logger.Warn("read error during cleanup", "error", err, "node", oldNode.Config.Node.ServerAddress)
						return
					}

					if !bytes.HasPrefix(delResp, []byte("OK")) {
						c.Logger.Warn("delete error during cleanup", "response", string(delResp), "node", oldNode.Config.Node.ServerAddress)
						return
					}

					c.Logger.Info("deleted stale key", "key", string(keyToDelete), "node", oldNode.Config.Node.ServerAddress)
				}(response.Node, key)
			}

			// Update our response to the more recent one
			response = resp
		}
		responseLock.Unlock()
	}

	// Check if we found any response
	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	// Format the response data
	return []byte(fmt.Sprintf("OK %s", response.Data)), nil
}

// ParallelRegx reads from all replicas in parallel using REGX command
// replaces duplicate keys comparing timestamps
func (c *Cluster) ParallelRegx(command []byte) ([]byte, error) {
	// We collect results with keys as map keys for easy deduplication
	resultMap := make(map[string]*struct {
		TimeStamp time.Time
		Key       []byte
		Data      []byte
		Node      *NodeConnection
	})

	wg := sync.WaitGroup{}
	resultLock := sync.Mutex{}

	// Process all nodes
	for _, nodeConn := range c.NodeConnections {
		nodeConn.Lock.Lock()

		if !nodeConn.Health {
			nodeConn.Lock.Unlock()

			// Process replicas if primary is down
			for _, replicaConn := range nodeConn.Replicas {
				replicaConn.Lock.Lock()

				if !replicaConn.Health {
					replicaConn.Lock.Unlock()
					continue
				}

				wg.Add(1)
				go func(nodeConn *NodeConnection, replicaConn *ReplicaConnection) {
					defer wg.Done()
					defer replicaConn.Lock.Unlock()

					// Send the command
					err := replicaConn.Client.Send(replicaConn.Context, command)
					if err != nil {
						c.Logger.Warn("write error", "error", err, "replica", replicaConn.Config.ServerAddress)
						return
					}

					// Receive the response
					rec, err := replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err, "replica", replicaConn.Config.ServerAddress)
						return
					}

					// Process successful response
					if bytes.HasPrefix(rec, []byte("OK")) {
						results := bytes.Split(rec, []byte("\r\n"))
						for i, result := range results {
							if len(result) == 0 {
								continue
							}

							// Parse the result line
							var timestampBytes []byte
							var keyBytes []byte
							var dataBytes []byte

							parts := bytes.Split(result, []byte(" "))
							if len(parts) < 2 {
								continue
							}

							if i == 0 { // First line format OK <timestamp> <key> <data>
								if len(parts) < 3 {
									continue
								}
								timestampBytes = parts[1]
								keyBytes = parts[2]
								dataBytes = bytes.Join(parts[3:], []byte(" "))
							} else { // Other lines format <timestamp> <key> <data>
								timestampBytes = parts[0]
								keyBytes = parts[1]
								dataBytes = bytes.Join(parts[2:], []byte(" "))
							}

							// Parse timestamp
							ts, err := time.Parse(time.RFC3339, string(timestampBytes))
							if err != nil {
								c.Logger.Warn("time parse error", "error", err, "replica", replicaConn.Config.ServerAddress)
								continue
							}

							// Add to results map with deduplication
							resultLock.Lock()
							keyStr := string(keyBytes)
							existing, exists := resultMap[keyStr]

							if !exists || ts.After(existing.TimeStamp) {
								// Replace or add new entry
								resultMap[keyStr] = &struct {
									TimeStamp time.Time
									Key       []byte
									Data      []byte
									Node      *NodeConnection
								}{
									TimeStamp: ts,
									Key:       keyBytes,
									Data:      dataBytes,
									Node:      nodeConn,
								}

								// If we had an older version, schedule it for deletion
								if exists && !bytes.Equal(existing.Key, keyBytes) {
									go func(n *NodeConnection, k []byte) {
										n.Lock.Lock()
										defer n.Lock.Unlock()
										if !n.Health {
											return
										}

										err := n.Client.Send(n.Context, []byte(fmt.Sprintf("DEL %s", k)))
										if err != nil {
											return
										}

										// Just receive without checking
										_, _ = n.Client.Receive(n.Context)
									}(existing.Node, existing.Key)
								}
							}
							resultLock.Unlock()
						}
					}
				}(nodeConn, replicaConn)
			}
		} else {
			// Process healthy primary node
			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()
				defer nodeConn.Lock.Unlock()

				// Send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// Receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
					return
				}

				// Process successful response
				if bytes.HasPrefix(rec, []byte("OK")) {
					results := bytes.Split(rec, []byte("\r\n"))
					for i, result := range results {
						if len(result) == 0 {
							continue
						}

						// Parse the result line
						var timestampBytes []byte
						var keyBytes []byte
						var dataBytes []byte

						parts := bytes.Split(result, []byte(" "))
						if len(parts) < 2 {
							continue
						}

						if i == 0 { // First line format OK <timestamp> <key> <data>
							if len(parts) < 3 {
								continue
							}
							timestampBytes = parts[1]
							keyBytes = parts[2]
							dataBytes = bytes.Join(parts[3:], []byte(" "))
						} else { // Other lines format <timestamp> <key> <data>
							timestampBytes = parts[0]
							keyBytes = parts[1]
							dataBytes = bytes.Join(parts[2:], []byte(" "))
						}

						// Parse timestamp
						ts, err := time.Parse(time.RFC3339, string(timestampBytes))
						if err != nil {
							c.Logger.Warn("time parse error", "error", err, "node", nodeConn.Config.Node.ServerAddress)
							continue
						}

						// Add to results map with deduplication
						resultLock.Lock()
						keyStr := string(keyBytes)
						existing, exists := resultMap[keyStr]

						if !exists || ts.After(existing.TimeStamp) {
							// Replace or add new entry
							resultMap[keyStr] = &struct {
								TimeStamp time.Time
								Key       []byte
								Data      []byte
								Node      *NodeConnection
							}{
								TimeStamp: ts,
								Key:       keyBytes,
								Data:      dataBytes,
								Node:      nodeConn,
							}

							// If we had an older version, schedule it for deletion
							if exists && !bytes.Equal(existing.Key, keyBytes) {
								go func(n *NodeConnection, k []byte) {
									n.Lock.Lock()
									defer n.Lock.Unlock()
									if !n.Health {
										return
									}

									err := n.Client.Send(n.Context, []byte(fmt.Sprintf("DEL %s", k)))
									if err != nil {
										return
									}

									// Just receive without checking
									_, _ = n.Client.Receive(n.Context)
								}(existing.Node, existing.Key)
							}
						}
						resultLock.Unlock()
					}
				}
			}(nodeConn)
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check if we found any results
	if len(resultMap) == 0 {
		return []byte("ERR no keys found\r\n"), nil
	}

	// Format final response
	var responseArray [][]byte
	responseArray = append(responseArray, []byte("OK"))

	// Convert map to array of results
	for _, result := range resultMap {
		responseArray = append(responseArray, []byte(fmt.Sprintf("%s %s", result.Key, result.Data)))
	}

	// Join with CRLF
	response := bytes.Join(responseArray, []byte("\r\n"))
	response = append(response, []byte("\r\n")...)

	return response, nil
}

// WriteToNode writes to a primary node in sequence
// Always starts at 0 and goes up to connected node count
func (c *Cluster) WriteToNode(data []byte) ([]byte, error) {
	// Handle single node case first
	if len(c.NodeConnections) == 1 {

		nodeConn := c.NodeConnections[0]
		if !nodeConn.Health {
			return nil, fmt.Errorf("node is down")
		}
		return c.sendToNode(nodeConn, data)
	}

	// Try writing to nodes starting from current sequence
	startSeq := c.Sequence.Load()
	attempts := 0
	maxAttempts := len(c.NodeConnections)

	for attempts < maxAttempts {
		seq := (startSeq + int32(attempts)) % int32(len(c.NodeConnections))
		nodeConn := c.NodeConnections[seq]

		if nodeConn.Health {
			response, err := c.sendToNode(nodeConn, data)
			if err == nil {
				// Only update sequence on successful write
				c.Sequence.Store((seq + 1) % int32(len(c.NodeConnections)))
				return response, nil
			}
		}

		attempts++
	}

	return nil, fmt.Errorf("no healthy nodes available")
}

// sendToNode sends data to a node and returns the response
func (c *Cluster) sendToNode(nodeConn *NodeConnection, data []byte) ([]byte, error) {
	if err := nodeConn.Client.Send(nodeConn.Context, data); err != nil {
		return nil, err
	}
	return nodeConn.Client.Receive(nodeConn.Context)
}

// ReloadConfig reloads a config file and propagates updates the cluster and all nodes in the chain
func (c *Cluster) ReloadConfig() error {
	c.ConfigLock.Lock()
	defer c.ConfigLock.Unlock()

	// Open the existing config file
	config, err := openExistingConfigFile(c.Wd)
	if err != nil {
		return err
	}

	// Update the cluster config
	c.Config = config

	// Update the server config
	c.Server.Config = config.ServerConfig

	// We need to lock the node connections list before modifying it
	c.NodeConnectionsLock.Lock()
	defer c.NodeConnectionsLock.Unlock()

	// Track existing node connections by server address for faster lookup
	existingNodes := make(map[string]*NodeConnection)
	for _, nodeConn := range c.NodeConnections {
		existingNodes[nodeConn.Config.Node.ServerAddress] = nodeConn
	}

	// Add new nodes that don't exist in current connections
	for _, nodeConfig := range config.NodeConfigs {
		serverAddr := nodeConfig.Node.ServerAddress

		if _, exists := existingNodes[serverAddr]; exists {
			// Node already exists, skip
			continue
		}

		// Create new node connection
		nodeConn := &NodeConnection{
			Config: nodeConfig,
			Health: false,
			Lock:   &sync.Mutex{},
		}

		// Add replicas
		for _, replicaConfig := range nodeConfig.Replicas {
			replicaConn := &ReplicaConnection{
				Config: replicaConfig,
				Health: false,
				Lock:   &sync.Mutex{},
			}
			nodeConn.Replicas = append(nodeConn.Replicas, replicaConn)
		}

		// Add to connections list
		c.NodeConnections = append(c.NodeConnections, nodeConn)
	}

	// Create a set of server addresses in the new config for fast lookup
	newConfigNodes := make(map[string]bool)
	for _, nodeConfig := range config.NodeConfigs {
		newConfigNodes[nodeConfig.Node.ServerAddress] = true
	}

	// Find and remove nodes that no longer exist in the config
	var updatedConnections []*NodeConnection
	for _, nodeConn := range c.NodeConnections {
		if !newConfigNodes[nodeConn.Config.Node.ServerAddress] {
			// Node no longer in config, close and skip
			if nodeConn.Client != nil && nodeConn.Health {
				nodeConn.Lock.Lock()
				if err := nodeConn.Client.Close(); err != nil {
					c.Logger.Warn("error closing node client",
						"error", err,
						"node", nodeConn.Config.Node.ServerAddress)
				}
				nodeConn.Lock.Unlock()
			}
		} else {
			// Keep this node
			updatedConnections = append(updatedConnections, nodeConn)
		}
	}

	// Update connections list
	c.NodeConnections = updatedConnections

	// Send RCNF command to all nodes - must be done after releasing the NodeConnectionsLock
	// to avoid potential deadlocks
	c.NodeConnectionsLock.Unlock()
	defer c.NodeConnectionsLock.Lock() // Reacquire before returning

	var reloadErr error
	wg := sync.WaitGroup{}

	// Send reload command to all nodes in parallel
	for _, nodeConn := range updatedConnections {
		if !nodeConn.Health {
			continue
		}

		wg.Add(1)
		go func(nc *NodeConnection) {
			defer wg.Done()

			nc.Lock.Lock()
			defer nc.Lock.Unlock()

			if !nc.Health {
				return
			}

			if err := nc.Client.Send(nc.Context, []byte("RCNF\r\n")); err != nil {
				c.Logger.Warn("failed to send reload command",
					"error", err,
					"node", nc.Config.Node.ServerAddress)
				reloadErr = err
				return
			}

			// Receive response but don't block on errors
			_, _ = nc.Client.Receive(nc.Context)

			// Send to replicas
			for _, replicaConn := range nc.Replicas {
				if !replicaConn.Health {
					continue
				}

				replicaConn.Lock.Lock()
				if err := replicaConn.Client.Send(replicaConn.Context, []byte("RCNF\r\n")); err != nil {
					c.Logger.Warn("failed to send reload command to replica",
						"error", err,
						"replica", replicaConn.Config.ServerAddress)
					replicaConn.Lock.Unlock()
					continue
				}

				// Receive response but don't block on errors
				_, _ = replicaConn.Client.Receive(replicaConn.Context)
				replicaConn.Lock.Unlock()
			}
		}(nodeConn)
	}

	// Wait for all reload commands to complete
	wg.Wait()

	return reloadErr
}
