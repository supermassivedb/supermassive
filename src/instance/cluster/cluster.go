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
}

// NodeConnection is the connection to a node
type NodeConnection struct {
	Client   *client.Client       // Is the connection to the master node
	Replicas []*ReplicaConnection // Are the connections to the read replicas
	Health   bool                 // Is the health status of the node
	Context  context.Context      // Is the context for the node
	Config   *NodeConfig          // Is the node configuration
}

// ReplicaConnection is a connection to a nodes read replica
type ReplicaConnection struct {
	Client  *client.Client  // Is the connection to the read replica
	Health  bool            // Is the health status of the read replica
	Context context.Context // Is the context for the read replica
	Config  *client.Config  // Is the read replica configuration
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

	return &Cluster{Logger: logger, SharedKey: sharedKey, Username: username, Password: password, Sequence: atomic.Int32{}, ConfigLock: &sync.RWMutex{}, NodeConnectionsLock: &sync.RWMutex{}}, nil
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
		}

		for _, replicaConfig := range nodeConfig.Replicas {
			replicaConn := &ReplicaConnection{
				Config: replicaConfig,
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
							if err != nil || string(response) != "OK authenticated\r\n" {
								c.Logger.Warn("authentication error", "error", err)
							} else {
								nodeConn.Health = true
								c.Logger.Info("node is reconnected and healthy", "node", nodeConn.Config.Node.ServerAddress)
							}
						}
					}
				} else {
					// We ping
					err := nodeConn.Client.Send(nodeConn.Context, []byte("PING\r\n"))
					if err != nil {
						// Mark the node as unhealthy
						nodeConn.Health = false
					}

					// We read response
					response, err := nodeConn.Client.Receive(nodeConn.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err)
						nodeConn.Health = false
						continue
					}

					// We check if the response is "OK PONG\r\n"
					if string(response) != "OK PONG\r\n" {
						c.Logger.Warn("read error", "error", err)
						// Mark the node as unhealthy
						nodeConn.Health = false
						continue
					}

				}

				for _, replicaConn := range nodeConn.Replicas {
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
								continue
							}
							response, err := replicaConn.Client.Receive(replicaConn.Context)
							if err != nil || string(response) != "OK authenticated\r\n" {
								c.Logger.Warn("replica authentication error", "error", err)
								continue
							}
							replicaConn.Health = true
							c.Logger.Info("replica is reconnected and healthy", "replica", replicaConn.Config.ServerAddress)
						}
					} else {

						// We ping
						err := replicaConn.Client.Send(replicaConn.Context, []byte("PING\r\n"))
						if err != nil {
							// Mark the node as unhealthy
							replicaConn.Health = false
						}

						// We read response
						response, err := replicaConn.Client.Receive(replicaConn.Context)
						if err != nil {
							c.Logger.Warn("read error", "error", err)
							replicaConn.Health = false
							continue
						}

						// We check if the response is "OK PONG\r\n"
						if string(response) != "OK PONG\r\n" {
							c.Logger.Warn("read error", "error", err)
							// Mark the node as unhealthy
							replicaConn.Health = false
							continue
						}
					}
				}
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
		_ = conn.SetReadDeadline(time.Time{})
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
					h.Cluster.Logger.Warn("write error", "error", err, "remote_addr", conn.RemoteAddr())
					return
				}
			} else {

				_, err = conn.Write(response)
				if err != nil {
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

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}

	for _, nodeConn := range c.NodeConnections {
		if nodeConn.Health {

			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()

				// We send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err)
					return
				}

				// We receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err)
					return
				}

				// If OK
				if bytes.HasPrefix(rec, []byte("OK")) {

					// We parse the received response
					timestamp := bytes.Split(rec, []byte(" "))[1]
					data := bytes.Join(bytes.Split(rec, []byte(" "))[2:], []byte(" "))

					responseInner := &struct {
						TimeStamp time.Time
						Data      []byte
						Node      *NodeConnection
					}{}

					responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestamp))
					if err != nil {
						c.Logger.Warn("time parse error", "error", err)
						return
					}

					responseInner.Data = data
					responseInner.Node = nodeConn

					lock.Lock()
					defer lock.Unlock()

					if response != nil {
						// We compare the timestamps
						if response.TimeStamp.After(responseInner.TimeStamp) {
							// We send a delete command to the primary node

							key := bytes.Split(command, []byte(" "))[1]

							err = response.Node.Client.Send(response.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
							if err != nil {
								c.Logger.Warn("write error", "error", err)
								return
							}

							_, err := response.Node.Client.Receive(response.Node.Context)
							if err != nil {
								c.Logger.Warn("read error", "error", err)
								return
							}
						}
					}
				}
			}(nodeConn)
		}
	}

	wg.Wait()

	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	return response.Data, nil
}

// Stats get stats on the cluster and it's nodes
func (c *Cluster) Stats() []byte {
	response := []byte("OK\r\n")

	// We get the cluster stats
	response = append(response, c.clusterStats()...)

	// We send a stat command to all nodes and form a response
	for _, nodeConn := range c.NodeConnections {
		response = append(response, fmt.Sprintf("PRIMARY %s\r\n", nodeConn.Config.Node.ServerAddress)...)
		if nodeConn.Health {
			// We send the command
			err := nodeConn.Client.Send(nodeConn.Context, []byte("STAT\r\n"))
			if err != nil {
				c.Logger.Warn("write error", "error", err)
				return []byte("ERR write error\r\n")
			}

			// We receive the response
			rec, err := nodeConn.Client.Receive(nodeConn.Context)
			if err != nil {
				c.Logger.Warn("read error", "error", err)
				return []byte("ERR read error\r\n")
			}

			rec = bytes.TrimPrefix(rec, []byte("OK\r\n"))

			response = append(response, rec...)
		} else {

			response = append(response, []byte("down\r\n")...)
		}

		// We check the replicas
		for _, replicaConn := range nodeConn.Replicas {
			response = append(response, fmt.Sprintf("REPLICA %s\r\n", replicaConn.Config.ServerAddress)...)
			if replicaConn.Health {
				// We send the command
				err := replicaConn.Client.Send(replicaConn.Context, []byte("STAT\r\n"))
				if err != nil {
					c.Logger.Warn("write error", "error", err)
					return []byte("ERR write error\r\n")
				}

				// We receive the response
				rec, err := replicaConn.Client.Receive(replicaConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err)
					return []byte("ERR read error\r\n")
				}

				rec = bytes.TrimPrefix(rec, []byte("OK\r\n"))

				response = append(response, rec...)
			} else {

				response = append(response, []byte("down\r\n")...)
			}

		}
	}

	return response
}

// ParallelIncrDecr increments a key in all primary nodes in parallel
func (c *Cluster) ParallelIncrDecr(command []byte) ([]byte, error) {
	// We incr from all primary nodes
	var response *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}

	for _, nodeConn := range c.NodeConnections {
		if nodeConn.Health {

			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()

				// We send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err)
					return
				}

				// We receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err)
					return
				}

				// If OK
				if bytes.HasPrefix(rec, []byte("OK")) {

					// We parse the received response
					timestamp := bytes.Split(rec, []byte(" "))[1]
					data := bytes.Join(bytes.Split(rec, []byte(" "))[2:], []byte(" "))

					responseInner := &struct {
						TimeStamp time.Time
						Data      []byte
						Node      *NodeConnection
					}{}

					responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestamp))
					if err != nil {
						c.Logger.Warn("time parse error", "error", err)
						return
					}

					responseInner.Data = data
					responseInner.Node = nodeConn

					lock.Lock()
					defer lock.Unlock()

					if response != nil {
						// We compare the timestamps
						if response.TimeStamp.After(responseInner.TimeStamp) {
							// We send a delete command to the primary node

							key := bytes.Split(command, []byte(" "))[1]

							err = response.Node.Client.Send(response.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
							if err != nil {
								c.Logger.Warn("write error", "error", err)
								return
							}

							// We get response from node assure it's ok log it
							nodeResponse, err := response.Node.Client.Receive(response.Node.Context)
							if err != nil {
								c.Logger.Warn("read error", "error", err)
								return
							}

							if string(nodeResponse) != "OK key-value deleted\r\n" {
								c.Logger.Warn("delete error", "error", err)
								return
							}
							c.Logger.Info("key deleted", "key", string(key))
						}
					}

					response = responseInner
				}
			}(nodeConn)
		}

	}

	wg.Wait()

	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	return response.Data, nil

}

// ParallelGet reads from all replicas in parallel
func (c *Cluster) ParallelGet(command []byte) ([]byte, error) {
	// We get from all primary replicas
	var response *struct {
		TimeStamp time.Time
		Data      []byte
		Node      *NodeConnection
	}

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}

	for _, nodeConn := range c.NodeConnections {

		if !nodeConn.Health {
			// We check replica if primary is down
			for _, replicaConn := range nodeConn.Replicas {
				if !replicaConn.Health {
					continue
				}

				wg.Add(1)
				go func(nodeConn *NodeConnection, replicaConn *ReplicaConnection) {
					defer wg.Done()

					// We send the command
					err := replicaConn.Client.Send(replicaConn.Context, command)
					if err != nil {
						c.Logger.Warn("write error", "error", err)
						return
					}

					// We receive the response
					rec, err := replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err)
						return
					}

					// If OK
					if bytes.HasPrefix(rec, []byte("OK")) {

						// We parse the received response
						timestamp := bytes.Split(rec, []byte(" "))[1]
						data := bytes.Join(bytes.Split(rec, []byte(" "))[2:], []byte(" "))

						responseInner := &struct {
							TimeStamp time.Time
							Data      []byte
							Node      *NodeConnection
						}{}

						responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestamp))
						if err != nil {
							c.Logger.Warn("time parse error", "error", err)
							return
						}

						responseInner.Data = data
						responseInner.Node = nodeConn

						lock.Lock()
						defer lock.Unlock()

						if response != nil {
							// We compare the timestamps
							if response.TimeStamp.After(responseInner.TimeStamp) {
								// We send a delete command to the primary node

								key := bytes.Split(command, []byte(" "))[1]

								err = response.Node.Client.Send(response.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
								if err != nil {
									c.Logger.Warn("write error", "error", err)
									return
								}

								// We get response from node assure it's ok log it
								nodeResponse, err := response.Node.Client.Receive(response.Node.Context)
								if err != nil {
									c.Logger.Warn("read error", "error", err)
									return
								}

								if string(nodeResponse) != "OK key-value deleted\r\n" {
									c.Logger.Warn("delete error", "error", err)
									return
								}
								c.Logger.Info("key deleted", "key", string(key))
							}
						}

						response = responseInner
					}
				}(nodeConn, replicaConn)

			}
			continue
		} else {

			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()

				// We send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err)
					return
				}

				// We receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err)
					return
				}

				// If OK
				if bytes.HasPrefix(rec, []byte("OK")) {

					// We parse the received response
					timestamp := bytes.Split(rec, []byte(" "))[1]
					data := bytes.Join(bytes.Split(rec, []byte(" "))[2:], []byte(" "))

					responseInner := &struct {
						TimeStamp time.Time
						Data      []byte
						Node      *NodeConnection
					}{}

					responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestamp))
					if err != nil {
						c.Logger.Warn("time parse error", "error", err)
						return
					}

					responseInner.Data = data
					responseInner.Node = nodeConn

					lock.Lock()
					defer lock.Unlock()

					if response != nil {
						// We compare the timestamps
						if response.TimeStamp.After(responseInner.TimeStamp) {
							// We send a delete command to the node
							key := bytes.Split(command, []byte(" "))[1]

							err = response.Node.Client.Send(response.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
							if err != nil {
								c.Logger.Warn("write error", "error", err)
								return
							}

							// We get response from node assure it's ok log it
							nodeResponse, err := response.Node.Client.Receive(response.Node.Context)
							if err != nil {
								c.Logger.Warn("read error", "error", err)
								return
							}

							if string(nodeResponse) != "OK key-value deleted\r\n" {
								c.Logger.Warn("delete error", "error", err)
								return
							}
							c.Logger.Info("key deleted", "key", string(key))
						}
					}

					response = responseInner
				}
			}(nodeConn)
		}
	}

	wg.Wait()

	if response == nil {
		return []byte("ERR key not found\r\n"), nil
	}

	return response.Data, nil
}

// ParallelRegx reads from all replicas in parallel using REGX command
// replaces duplicate keys comparing timestamps
func (c *Cluster) ParallelRegx(command []byte) ([]byte, error) {
	// We try to get from primary if not avail we try replica to command
	var response []*struct {
		TimeStamp time.Time
		Key       []byte
		Data      []byte
		Node      *NodeConnection
	}

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}

	for _, nodeConn := range c.NodeConnections {

		if !nodeConn.Health {
			// We check replica if primary is down
			for _, replicaConn := range nodeConn.Replicas {
				if !replicaConn.Health {
					continue
				}

				wg.Add(1)
				go func(nodeConn *NodeConnection, replicaConn *ReplicaConnection) {
					defer wg.Done()

					// We send the command
					err := replicaConn.Client.Send(replicaConn.Context, command)
					if err != nil {
						c.Logger.Warn("write error", "error", err)
						return
					}

					// We receive the response
					rec, err := replicaConn.Client.Receive(replicaConn.Context)
					if err != nil {
						c.Logger.Warn("read error", "error", err)
						return
					}

					// If OK
					if bytes.HasPrefix(rec, []byte("OK")) {

						// We split by CRLF
						results := bytes.Split(rec, []byte("\r\n"))
						for i, result := range results {
							var timestampBytes []byte
							var data []byte

							responseInner := &struct {
								TimeStamp time.Time
								Key       []byte
								Data      []byte
								Node      *NodeConnection
							}{}

							// We parse the received response
							if i == 0 {
								timestampBytes = bytes.Split(result, []byte(" "))[1]
								data = bytes.Join(bytes.Split(result, []byte(" "))[2:], []byte(" "))
								responseInner.Key = bytes.Split(result, []byte(" "))[2]
							} else {
								timestampBytes = bytes.Split(result, []byte(" "))[0]
								data = bytes.Join(bytes.Split(result, []byte(" "))[1:], []byte(" "))
								responseInner.Key = bytes.Split(result, []byte(" "))[1]
							}

							responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestampBytes))
							if err != nil {
								c.Logger.Warn("time parse error", "error", err)
								return
							}

							responseInner.Data = data
							responseInner.Node = nodeConn

							if response != nil {
								// We check the response results for the key, if its there we compare
								for j, res := range response {
									if bytes.Equal(res.Key, responseInner.Key) {
										// We compare the timestamps
										if res.TimeStamp.After(responseInner.TimeStamp) {
											// We send a delete command to the primary node

											key := responseInner.Key

											err = responseInner.Node.Client.Send(responseInner.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
											if err != nil {
												c.Logger.Warn("write error", "error", err)
												return
											}

											// We get response from node assure it's ok log it
											nodeResponse, err := responseInner.Node.Client.Receive(responseInner.Node.Context)
											if err != nil {
												c.Logger.Warn("read error", "error", err)
												return
											}

											if string(nodeResponse) != "OK key-value deleted\r\n" {
												c.Logger.Warn("delete error", "error", err)
												return
											}
											c.Logger.Info("key deleted", "key", string(key))

											// We replace the response
											lock.Lock()

											response[j] = responseInner
											lock.Unlock()
										}
									}
								}

							}
						}

					}
				}(nodeConn, replicaConn)

			}
			continue
		} else {
			wg.Add(1)
			go func(nodeConn *NodeConnection) {
				defer wg.Done()

				// We send the command
				err := nodeConn.Client.Send(nodeConn.Context, command)
				if err != nil {
					c.Logger.Warn("write error", "error", err)
					return
				}

				// We receive the response
				rec, err := nodeConn.Client.Receive(nodeConn.Context)
				if err != nil {
					c.Logger.Warn("read error", "error", err)
					return
				}

				// If OK we parse the response
				if bytes.HasPrefix(rec, []byte("OK")) {

					// We split by CRLF
					results := bytes.Split(rec, []byte("\r\n"))
					for i, result := range results {
						var timestampBytes []byte
						var data []byte

						responseInner := &struct {
							TimeStamp time.Time
							Key       []byte
							Data      []byte
							Node      *NodeConnection
						}{}

						// We parse the received response
						if i == 0 {
							timestampBytes = bytes.Split(result, []byte(" "))[1]
							data = bytes.Join(bytes.Split(result, []byte(" "))[2:], []byte(" "))
							responseInner.Key = bytes.Split(result, []byte(" "))[2]
						} else {
							timestampBytes = bytes.Split(result, []byte(" "))[0]
							data = bytes.Join(bytes.Split(result, []byte(" "))[1:], []byte(" "))
							responseInner.Key = bytes.Split(result, []byte(" "))[1]
						}

						responseInner.TimeStamp, err = time.Parse(time.RFC3339, string(timestampBytes))
						if err != nil {
							c.Logger.Warn("time parse error", "error", err)
							return
						}

						responseInner.Data = data
						responseInner.Node = nodeConn

						if response != nil {
							// We check the response results for the key, if its there we compare
							for j, res := range response {
								if bytes.Equal(res.Key, responseInner.Key) {
									// We compare the timestamps
									if res.TimeStamp.After(responseInner.TimeStamp) {
										// We send a delete command to the primary node

										key := responseInner.Key

										err = responseInner.Node.Client.Send(responseInner.Node.Context, []byte(fmt.Sprintf("DEL %s", key)))
										if err != nil {
											c.Logger.Warn("write error", "error", err)
											return
										}

										// We get response from node assure it's ok log it
										nodeResponse, err := responseInner.Node.Client.Receive(responseInner.Node.Context)
										if err != nil {
											c.Logger.Warn("read error", "error", err)
											return
										}

										if string(nodeResponse) != "OK key-value deleted\r\n" {
											c.Logger.Warn("delete error", "error", err)
											return
										}
										c.Logger.Info("key deleted", "key", string(key))

										// We replace the response
										lock.Lock()

										response[j] = responseInner
										lock.Unlock()
									}
								}
							}

						}
					}

				}
			}(nodeConn)
		}
	}

	wg.Wait()

	if response == nil {
		return []byte("ERR no keys found\r\n"), nil
	}

	results := []byte("OK ")
	for _, res := range response {
		results = append(results, []byte(fmt.Sprintf("%s %s\r\n", res.Key, res.Data))...)
	}

	return results, nil
}

// WriteToNode writes to a primary node in sequence
// Always starts at 0 and goes up to connected node count
func (c *Cluster) WriteToNode(data []byte) ([]byte, error) {

	seq := c.Sequence.Load()

	// If there is just one node, we don't care for sequence
	if len(c.NodeConnections) == 1 {
		c.Logger.Info("single node, add more to scale")
		nodeConn := c.NodeConnections[0]

		if !nodeConn.Health && nodeConn.Context == nil && nodeConn.Client == nil {
			return nil, fmt.Errorf("node is down")
		}

		// We send the data to the node
		err := nodeConn.Client.Send(nodeConn.Context, data)
		if err != nil {
			return nil, err
		}

		// We receive the response
		response, err := nodeConn.Client.Receive(nodeConn.Context)
		if err != nil {
			return nil, err
		}

		return response, nil

	}

	c.Logger.Info("sequence", "seq", seq)

	nodeConn := c.NodeConnections[seq]

	if seq == int32(len(c.NodeConnections))-1 { // We reset the sequence if we reach the end
		seq = 0
		c.Sequence.Store(seq)
	} else {
		// We increment the sequence
		seq++
		c.Sequence.Store(seq)
	}

	if !nodeConn.Health && nodeConn.Context == nil && nodeConn.Client == nil {

		// Recursively call the function
		response, err := c.WriteToNode(data)
		if err != nil {
			return nil, err
		}

		return response, nil
	}

	// We send the data to the node
	err := nodeConn.Client.Send(nodeConn.Context, data)
	if err != nil {

		// Recursively call the function
		response, err := c.WriteToNode(data)
		if err != nil {
			return nil, err
		}

		return response, nil
	}

	// We receive the response
	response, err := nodeConn.Client.Receive(nodeConn.Context)
	if err != nil {

		return nil, err
	}

	return response, nil

}

// ReloadConfig reloads a config file and propagates updates the cluster and all nodes in the chain
func (c *Cluster) ReloadConfig() error {
	c.ConfigLock.Lock()
	defer c.ConfigLock.Unlock()
	// We open the existing config file
	config, err := openExistingConfigFile(c.Wd)
	if err != nil {
		return err
	}

	// We update the cluster config
	c.Config = config

	// We update the server config
	c.Server.Config = config.ServerConfig

	for _, nodeConfig := range config.NodeConfigs {
		// We check if the node connection already exists
		var exists bool
		for _, nodeConn := range c.NodeConnections {
			if nodeConn.Config.Node.ServerAddress == nodeConfig.Node.ServerAddress {
				exists = true
				break
			}
		}

		if exists {
			continue
		}

		nodeConn := &NodeConnection{
			Config: nodeConfig,
		}

		c.NodeConnections = append(c.NodeConnections, nodeConn)
	}

	// Now we find what nodes to remove
	var remove []int
	for i, nodeConn := range c.NodeConnections {
		var found bool
		for _, nodeConfig := range config.NodeConfigs {
			if nodeConn.Config.Node.ServerAddress == nodeConfig.Node.ServerAddress {
				found = true
				break
			}
		}

		if !found {
			remove = append(remove, i)
		}
	}

	// We remove the nodes
	for _, i := range remove {
		// We close the client
		if c.NodeConnections[i].Client != nil {
			err = c.NodeConnections[i].Client.Close()
			if err != nil {
				return err
			}
		}
		c.NodeConnections = append(c.NodeConnections[:i], c.NodeConnections[i+1:]...)
	}

	// Send RELOAD command to all nodes
	for _, nodeConn := range c.NodeConnections {
		if nodeConn.Health {
			err = nodeConn.Client.Send(nodeConn.Context, []byte("RCNF\r\n"))
			if err != nil {
				return err
			}

			for _, replicaConn := range nodeConn.Replicas {
				if replicaConn.Health {
					err = replicaConn.Client.Send(replicaConn.Context, []byte("RCNF\r\n"))
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}
