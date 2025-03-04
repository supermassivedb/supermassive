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
package node

import (
	"context"
	"crypto/sha256"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"supermassive/instance/nodereplica"
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
		wantErr   bool
	}{
		{
			name:      "valid creation",
			logger:    logger,
			sharedKey: "test-key",
			wantErr:   false,
		},
		{
			name:      "missing shared key",
			logger:    logger,
			sharedKey: "",
			wantErr:   true,
		},
		{
			name:      "nil logger",
			logger:    nil,
			sharedKey: "test-key",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.logger, tt.sharedKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_Open(t *testing.T) {
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
				ReadReplicas: []*client.Config{
					{

						ServerAddress:  "localhost:0", // Use port 0 to let OS assign random port
						ConnectTimeout: 1,             // Reduced timeout for testing
						WriteTimeout:   1,
						ReadTimeout:    1,
						BufferSize:     1024,
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
				c, err := New(logger, "test-key")
				if err != nil {
					t.Errorf("Failed to create cluster: %v", err)
					close(done)
					return
				}
				c.Config = tt.config

				go func() {
					err = c.Open(nil)
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

				os.Remove(".node")
				os.Remove(".journal")
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

	if len(config.ReadReplicas) != len(configData.ReadReplicas) {
		t.Errorf("Expected %d ReadReplicas, got %d", len(configData.ReadReplicas), len(config.ReadReplicas))
	}

	if config.ReadReplicas[0].ServerAddress != configData.ReadReplicas[0].ServerAddress {
		t.Errorf("Expected ReadReplicas[0].ServerAddress %s, got %s", configData.ReadReplicas[0].ServerAddress, config.ReadReplicas[0].ServerAddress)
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

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	// We create a tcp client to the node, we know the default port is going to be 4001
	// Resolve the string address to a TCP address
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
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

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// We ping
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

func TestServerCrud(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT hello world\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("GET hello\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to get key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if !strings.Contains(string(buf[:n]), "hello world") {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("DEL hello\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to delete key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value deleted\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value deleted', got %s", string(buf[:n]))
	}

	conn.Close()

	nr.Close()

}

func TestServerIncrDecr(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT n 1\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("INCR n 1\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to incr key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if !strings.Contains(string(buf[:n]), "n 2") {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'n 2', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("DECR n 2\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to decr key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if !strings.Contains(string(buf[:n]), "n 0") {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'n 0', got %s", string(buf[:n]))
	}

	conn.Close()

	nr.Close()

}

func TestServerRegx(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT user:some_id1 user1 content\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT user:some_id2 user2 content\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT profile:some_id1 profile1 content\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("PUT profile:some_id2 profile2 content\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to write key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK key-value written\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("REGX ^user:some_id[0-9]+$\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to incr key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if !strings.Contains(string(buf[:n]), "user:some_id1") && !strings.Contains(string(buf[:n]), "user:some_id2") {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'user:some_id1 user:some_id2', got %s", string(buf[:n]))
	}

	conn.Close()

	nr.Close()

}

func TestServerStat(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	for i := 0; i < 100; i++ {
		_, err = conn.Write([]byte(fmt.Sprintf("PUT hello%d world\r\n", i)))
		if err != nil {
			conn.Close()
			nr.Close()
			t.Fatalf("Failed to write key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = conn.Read(buf)
		if err != nil {
			conn.Close()
			nr.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if string(buf[:n]) != "OK key-value written\r\n" {
			conn.Close()
			nr.Close()
			t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
		}
	}

	_, err = conn.Write([]byte(fmt.Sprintf("STAT\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to get key-value: %v", err)
	}

	buf = make([]byte, 1024)

	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	keywords := []string{
		"DISK",
		"last_page",
		"total_header_size",
		"header_overhead_ratio",
		"page_utilization",
		"file_name",
		"file_mode",
		"sync_enabled",
		"sync_interval",
		"total_pages",
		"total_data_size",
		"storage_efficiency",
		"file_size",
		"modified_time",
		"page_size",
		"is_closed",
		"avg_page_size",
		"MEMORY",
		"avg_probe_length",
		"utilization",
		"needs_shrink",
		"size",
		"load_factor",
		"grow_threshold",
		"shrink_threshold",
		"max_probe_length",
		"empty_buckets",
		"empty_bucket_ratio",
		"needs_grow",
		"used",
	}

	for _, keyword := range keywords {
		if !strings.Contains(string(buf[:n]), keyword) {
			conn.Close()
			nr.Close()
			t.Fatalf("Expected '%s', got %s", keyword, string(buf[:n]))
		}
	}

	conn.Close()

	nr.Close()

}

func TestServerConfigRefresh(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// We create a new node
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	// We read the current config, modify it and then run RCNF
	config, err := os.ReadFile(".node")
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to read config file: %v", err)
	}

	// We decode the config
	var c Config
	err = yaml.Unmarshal(config, &c)
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to unmarshal config file: %v", err)
	}

	// We modify the config
	c.ServerConfig.BufferSize = 2048

	// We encode the config
	data, err := yaml.Marshal(c)
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to marshal config data: %v", err)
	}

	// We truncate the file
	err = os.WriteFile(".node", data, 0644)
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to write config file: %v", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4001")
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
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	_, err = conn.Write([]byte(fmt.Sprintf("RCNF\r\n")))
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to refresh config: %v", err)
	}

	// We expect "OK configs reloaded" as response

	buf = make([]byte, 1024)
	n, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK configs reloaded\r\n" {
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK configs reloaded', got %s", string(buf[:n]))
	}

	conn.Close()

	if nr.Config.ServerConfig.BufferSize != 2048 {
		nr.Close()
		t.Fatalf("Expected buffer size 2048, got %d", nr.Config.ServerConfig.BufferSize)
	}

	nr.Close()
}

// We create a primary and 1 replicas.  Configure the primary for the 1 replica..
// We open the primary, we open the replica.. We write commands to the primary and we expect the replicas to have the same data.
func TestServerRelayToReplicas(t *testing.T) {
	var replica *nodereplica.NodeReplica
	var replica2 *nodereplica.NodeReplica

	go func() {
		// We make temp dir
		if err := os.Mkdir(".replica_test/", 0755); err != nil {
			return
		}

		var err error
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

		replica, err = nodereplica.New(logger, "test-key")
		if err != nil {
			return
		}

		replica.Config = &nodereplica.Config{
			ServerConfig: &server.Config{
				Address:     "localhost:4004",
				UseTLS:      false,
				ReadTimeout: 10,
				BufferSize:  1024,
			},
			MaxMemoryThreshold: 75,
		}

		dir := ".replica_test/"

		// Marhsal config into .replica_test/
		yamlRaw, err := yaml.Marshal(replica.Config)
		if err != nil {
			t.Errorf("Failed to marshal config: %v", err)
			return
		}

		err = os.WriteFile(".replica_test/.nodereplica", yamlRaw, 0644)
		if err != nil {
			t.Errorf("Failed to write config file: %v", err)
			return
		}

		err = replica.Open(&dir)
		if err != nil {
			t.Errorf("Failed to open node replica: %v", err)
			return
		}

	}()

	time.Sleep(time.Second * 1)

	go func() {
		// We make temp dir
		if err := os.Mkdir(".replica_test2/", 0755); err != nil {
			return
		}

		var err error
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

		replica2, err = nodereplica.New(logger, "test-key")
		if err != nil {
			return
		}

		replica2.Config = &nodereplica.Config{
			ServerConfig: &server.Config{
				Address:     "localhost:4005",
				UseTLS:      false,
				ReadTimeout: 10,
				BufferSize:  1024,
			},
			MaxMemoryThreshold: 75,
		}

		// Marhsal config into .replica_test2/

		yamlRaw, err := yaml.Marshal(replica2.Config)
		if err != nil {
			t.Errorf("Failed to marshal config: %v", err)
			return
		}

		err = os.WriteFile(".replica_test2/.nodereplica", yamlRaw, 0644)
		if err != nil {
			t.Errorf("Failed to write config file: %v", err)
			return
		}

		dir := ".replica_test2/"

		err = replica2.Open(&dir)
		if err != nil {
			t.Errorf("Failed to open node replica: %v", err)
			return
		}

	}()

	defer os.RemoveAll(".replica_test/")
	defer os.RemoveAll(".replica_test2/")

	time.Sleep(1 * time.Second)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	primaryConfig := `health-check-interval: 1
max-memory-threshold: 75
server-config:
    address: localhost:4006
    use-tls: false
    cert-file: /
    key-file: /
    read-timeout: 10
    buffer-size: 1024
read-replicas:
    - server-address: localhost:4004
      use-tls: false
      ca-cert-file: /
      connect-timeout: 5
      write-timeout: 5
      read-timeout: 5
      max-retries: 3
      retry-wait-time: 1
      buffer-size: 1024
    - server-address: localhost:4005
      use-tls: false
      ca-cert-file: /
      connect-timeout: 5
      write-timeout: 5
      read-timeout: 5
      max-retries: 3
      retry-wait-time: 1
      buffer-size: 1024
`

	// We write primary config
	err := os.WriteFile(".node", []byte(primaryConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write primary config file: %v", err)
	}

	// We create a new node primary
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4006")
	if err != nil {
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to connect to server: %v", err)

	}

	// We authenticate
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	for i := 0; i < 100; i++ {
		_, err = conn.Write([]byte(fmt.Sprintf("PUT hello%d world%d\r\n", i, i)))
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Failed to write key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = conn.Read(buf)
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if string(buf[:n]) != "OK key-value written\r\n" {
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
		}
	}

	// We connect to replica2 and check if all the data is there
	tcpAddrRep2, err := net.ResolveTCPAddr("tcp4", "localhost:4005")
	if err != nil {
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	connRep2, err := net.DialTCP("tcp", nil, tcpAddrRep2)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// We authenticate
	_, err = connRep2.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		connRep2.Close()
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf = make([]byte, 1024)

	n, err = connRep2.Read(buf)
	if err != nil {
		connRep2.Close()
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		connRep2.Close()
		conn.Close()
		nr.Close()
		replica.Close()
		replica2.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	for i := 0; i < 100; i++ {
		_, err = connRep2.Write([]byte(fmt.Sprintf("GET hello%d\r\n", i)))
		if err != nil {
			connRep2.Close()
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Failed to get key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = connRep2.Read(buf)
		if err != nil {
			connRep2.Close()
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if !strings.Contains(string(buf[:n]), fmt.Sprintf("hello%d world%d", i, i)) {
			connRep2.Close()
			conn.Close()
			nr.Close()
			replica.Close()
			replica2.Close()
			t.Fatalf("Expected 'hello%d world%d', got %s", i, i, string(buf[:n]))
		}

	}

	connRep2.Close()
	conn.Close()
	nr.Close()

	replica.Close()
	replica2.Close()

}

// We create a primary and 1 replica.  We configure the primary for the 1 replica.
// We write a few commands to the primary and we expect the replica to have the same data,
// then we close the replica, and continue to write commands to primary.  After we turn on the replica, we expect the replica to have the same data
// after sync.
func TestServerReplicaSync(t *testing.T) {
	var replica *nodereplica.NodeReplica

	go func() {
		// We make temp dir
		if err := os.Mkdir(".replica_test/", 0755); err != nil {
			return
		}

		var err error
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

		replica, err = nodereplica.New(logger, "test-key")
		if err != nil {
			return
		}

		replica.Config = &nodereplica.Config{
			ServerConfig: &server.Config{
				Address:     "localhost:4007",
				UseTLS:      false,
				ReadTimeout: 10,
				BufferSize:  1024,
			},
			MaxMemoryThreshold: 75,
		}

		dir := ".replica_test/"

		// Marhsal config into .replica_test/
		yamlRaw, err := yaml.Marshal(replica.Config)
		if err != nil {
			t.Errorf("Failed to marshal config: %v", err)
			return
		}

		err = os.WriteFile(".replica_test/.nodereplica", yamlRaw, 0644)
		if err != nil {
			t.Errorf("Failed to write config file: %v", err)
			return
		}

		err = replica.Open(&dir)
		if err != nil {
			t.Errorf("Failed to open node replica: %v", err)
			return
		}

	}()

	time.Sleep(time.Second * 1)

	defer os.RemoveAll(".replica_test/")

	time.Sleep(1 * time.Second)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	primaryConfig := `health-check-interval: 1
max-memory-threshold: 75
server-config:
    address: localhost:4008
    use-tls: false
    cert-file: /
    key-file: /
    read-timeout: 10
    buffer-size: 1024
read-replicas:
    - server-address: localhost:4007
      use-tls: false
      ca-cert-file: /
      connect-timeout: 5
      write-timeout: 5
      read-timeout: 5
      max-retries: 3
      retry-wait-time: 1
      buffer-size: 1024
`

	// We write primary config
	err := os.WriteFile(".node", []byte(primaryConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write primary config file: %v", err)
	}

	// We create a new node primary
	nr, err := New(logger, "test-key")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// We open in background
	go func() {
		err := nr.Open(nil)
		if err != nil {
			t.Fatalf("Failed to open node: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	defer os.Remove(".journal")
	defer os.Remove(".node")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4008")
	if err != nil {
		nr.Close()
		replica.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		replica.Close()
		t.Fatalf("Failed to connect to server: %v", err)

	}

	// We authenticate
	_, err = conn.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		conn.Close()
		nr.Close()
		replica.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		nr.Close()
		replica.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		conn.Close()
		nr.Close()
		replica.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	for i := 0; i < 100; i++ {
		_, err = conn.Write([]byte(fmt.Sprintf("PUT hello%d world%d\r\n", i, i)))
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Failed to write key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = conn.Read(buf)
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if string(buf[:n]) != "OK key-value written\r\n" {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
		}
	}

	nr.ReplicaConnections[0].Client.Close()
	// Now we close the replica
	replica.Close()

	// Now we add more data to the primary
	for i := 100; i < 200; i++ {
		_, err = conn.Write([]byte(fmt.Sprintf("PUT hello%d world%d\r\n", i, i)))
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Failed to write key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = conn.Read(buf)
		if err != nil {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if string(buf[:n]) != "OK key-value written\r\n" {
			conn.Close()
			nr.Close()
			replica.Close()
			t.Fatalf("Expected 'OK key-value written', got %s", string(buf[:n]))
		}
	}
	//
	// Now we reopen the replica
	go func() {

		var err error
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

		replica, err = nodereplica.New(logger, "test-key")
		if err != nil {
			return
		}

		replica.Config = &nodereplica.Config{
			ServerConfig: &server.Config{
				Address:     "localhost:4007",
				UseTLS:      false,
				ReadTimeout: 10,
				BufferSize:  1024,
			},
			MaxMemoryThreshold: 75,
		}

		dir := ".replica_test/"

		// Marhsal config into .replica_test/
		yamlRaw, err := yaml.Marshal(replica.Config)
		if err != nil {
			t.Errorf("Failed to marshal config: %v", err)
			return
		}

		err = os.WriteFile(".replica_test/.nodereplica", yamlRaw, 0644)
		if err != nil {
			t.Errorf("Failed to write config file: %v", err)
			return
		}

		err = replica.Open(&dir)
		if err != nil {
			t.Errorf("Failed to open node replica: %v", err)
			return
		}

	}()

	time.Sleep(time.Second * 3) // We wait for sync

	// We connect to replica and check if all the data is there
	tcpAddrRep, err := net.ResolveTCPAddr("tcp4", "localhost:4007")
	if err != nil {
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	connRep, err := net.DialTCP("tcp", nil, tcpAddrRep)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// We authenticate
	_, err = connRep.Write([]byte(fmt.Sprintf("NAUTH %x\r\n", sha256.Sum256([]byte("test-key")))))
	if err != nil {
		connRep.Close()
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to authenticate: %v", err)
	}

	// We expect "OK authenticated" as response
	buf = make([]byte, 1024)

	n, err = connRep.Read(buf)
	if err != nil {
		connRep.Close()
		conn.Close()
		nr.Close()
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "OK authenticated\r\n" {
		connRep.Close()
		conn.Close()
		nr.Close()
		t.Fatalf("Expected 'OK authenticated', got %s", string(buf[:n]))
	}

	for i := 0; i < 200; i++ {
		_, err = connRep.Write([]byte(fmt.Sprintf("GET hello%d\r\n", i)))
		if err != nil {
			connRep.Close()
			conn.Close()
			nr.Close()
			t.Fatalf("Failed to get key-value: %v", err)
		}

		buf = make([]byte, 1024)

		n, err = connRep.Read(buf)
		if err != nil {
			connRep.Close()
			conn.Close()
			nr.Close()
			t.Fatalf("Failed to read response: %v", err)
		}

		if !strings.Contains(string(buf[:n]), fmt.Sprintf("hello%d world%d", i, i)) {
			connRep.Close()
			conn.Close()
			nr.Close()
			t.Fatalf("Expected 'hello%d world%d', got %s", i, i, string(buf[:n]))
		}
	}

	connRep.Close()

	conn.Close()
	nr.Close()
	replica.Close()
}
