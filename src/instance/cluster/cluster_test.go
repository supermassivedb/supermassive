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
package cluster

import (
	"context"
	"encoding/base64"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"supermassive/network/client"
	"supermassive/network/server"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tests := []struct {
		name      string
		logger    *slog.Logger
		sharedKey string
		username  string
		password  string
		wantErr   bool
	}{
		{
			name:      "valid creation",
			logger:    logger,
			sharedKey: "test-key",
			username:  "test-user",
			password:  "test-pass",
			wantErr:   false,
		},
		{
			name:      "missing shared key",
			logger:    logger,
			sharedKey: "",
			username:  "test-user",
			password:  "test-pass",
			wantErr:   true,
		},
		{
			name:      "missing username",
			logger:    logger,
			sharedKey: "test-key",
			username:  "",
			password:  "test-pass",
			wantErr:   true,
		},
		{
			name:      "missing password",
			logger:    logger,
			sharedKey: "test-key",
			username:  "test-user",
			password:  "",
			wantErr:   true,
		},
		{
			name:      "nil logger",
			logger:    nil,
			sharedKey: "test-key",
			username:  "test-user",
			password:  "test-pass",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.logger, tt.sharedKey, tt.username, tt.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCluster_Open(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create a context with timeout to prevent infinite hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				HealthCheckInterval: 2,
				ServerConfig: &server.Config{
					Address:     "localhost:0", // Use port 0 to let OS assign random port
					UseTLS:      false,
					ReadTimeout: 10,
					BufferSize:  1024,
				},
				NodeConfigs: []*NodeConfig{
					{
						Node: &client.Config{
							ServerAddress:  "localhost:0", // Use port 0 to let OS assign random port
							ConnectTimeout: 1,             // Reduced timeout for testing
							WriteTimeout:   1,
							ReadTimeout:    1,
							BufferSize:     1024,
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a channel to signal test completion
			done := make(chan struct{})

			go func() {
				c, err := New(logger, "test-key", "test-user", "test-pass")
				if err != nil {
					t.Errorf("Failed to create cluster: %v", err)
					close(done)
					return
				}
				c.Config = tt.config

				go func() {
					err = c.Open()
					if (err != nil) != tt.wantErr {
						t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
					}
				}()

				// Brief delay to allow health check goroutine to start
				time.Sleep(100 * time.Millisecond)

				err = c.Close()
				if err != nil {
					t.Errorf("Failed to close cluster: %v", err)
				}

				close(done)

				os.Remove(".cluster")
			}()

			// Wait for either test completion or timeout
			select {
			case <-done:
				// Test completed normally
			case <-ctx.Done():
				t.Fatal("Test timed out")
			}
		})
	}
}

func TestOpenExistingConfigFile(t *testing.T) {
	// Create a temporary directory
	tempDir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a temporary config file
	configFilePath := filepath.Join(tempDir, ConfigFile)
	configData := &Config{
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

	data, err := yaml.Marshal(configData)
	if err != nil {
		t.Fatalf("Failed to marshal config data: %v", err)
	}

	err = ioutil.WriteFile(configFilePath, data, 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Test the function
	config, err := openExistingConfigFile(tempDir)
	if err != nil {
		t.Fatalf("Failed to open existing config file: %v", err)
	}

	// Validate the config data
	if config.HealthCheckInterval != configData.HealthCheckInterval {
		t.Errorf("Expected HealthCheckInterval %d, got %d", configData.HealthCheckInterval, config.HealthCheckInterval)
	}

	if config.ServerConfig.Address != configData.ServerConfig.Address {
		t.Errorf("Expected ServerConfig.Address %s, got %s", configData.ServerConfig.Address, config.ServerConfig.Address)
	}

	if len(config.NodeConfigs) != len(configData.NodeConfigs) {
		t.Errorf("Expected %d NodeConfigs, got %d", len(configData.NodeConfigs), len(config.NodeConfigs))
	}

	if config.NodeConfigs[0].Node.ServerAddress != configData.NodeConfigs[0].Node.ServerAddress {
		t.Errorf("Expected Node.ServerAddress %s, got %s", configData.NodeConfigs[0].Node.ServerAddress, config.NodeConfigs[0].Node.ServerAddress)
	}

	if len(config.NodeConfigs[0].Replicas) != len(configData.NodeConfigs[0].Replicas) {
		t.Errorf("Expected %d Replicas, got %d", len(configData.NodeConfigs[0].Replicas), len(config.NodeConfigs[0].Replicas))
	}

	if config.NodeConfigs[0].Replicas[0].ServerAddress != configData.NodeConfigs[0].Replicas[0].ServerAddress {
		t.Errorf("Expected Replica.ServerAddress %s, got %s", configData.NodeConfigs[0].Replicas[0].ServerAddress, config.NodeConfigs[0].Replicas[0].ServerAddress)
	}
}

func TestCreateDefaultConfigFile(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Call the createDefaultConfigFile function
	_, err := createDefaultConfigFile(tempDir)
	if err != nil {
		t.Fatalf("Failed to create default config file: %v", err)
	}

}

func TestServerAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node replica
	nr, err := New(logger, "test-key", "test-user", "test-pass")
	if err != nil {
		t.Fatalf("Failed to create node replica: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open()
		if err != nil {
			t.Fatalf("Failed to open node replica: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".cluster")

	// We create a tcp client to the cluster, we know the default port is going to be 4000
	// Resolve the string address to a TCP address
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4000")
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	authStr := base64.StdEncoding.EncodeToString([]byte("test-user\\0test-pass"))

	// We authenticate
	_, err = conn.Write([]byte(fmt.Sprintf("AUTH %s\r\n", authStr)))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	conn.Close()

	if string(buf[:n]) != "OK authenticated\r\n" {
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	nr.Close()

}

func TestServerPing(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node replica
	nr, err := New(logger, "test-key", "test-user", "test-pass")
	if err != nil {
		t.Fatalf("Failed to create node replica: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open()
		if err != nil {
			t.Fatalf("Failed to open node replica: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".cluster")

	// We create a tcp client to the cluster, we know the default port is going to be 4000
	// Resolve the string address to a TCP address
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4000")
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// We authenticate
	_, err = conn.Write([]byte(fmt.Sprintf("PING\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	conn.Close()

	if string(buf[:n]) != "OK PONG\r\n" {
		nr.Close()
		t.Fatalf("Expected 'OK PONG', got %s", string(buf[:n]))
	}

	nr.Close()

}
