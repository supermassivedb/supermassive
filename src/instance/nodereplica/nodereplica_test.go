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

func TestNodeReplica_Open(t *testing.T) {
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
				ServerConfig: &server.Config{
					Address:     "localhost:0", // Use port 0 to let OS assign random port
					UseTLS:      false,
					ReadTimeout: 10,
					BufferSize:  1024,
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

				os.Remove(".nodereplica")
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
		ServerConfig: &server.Config{
			Address:     "localhost:4002",
			UseTLS:      false,
			CertFile:    "/",
			KeyFile:     "/",
			ReadTimeout: 10,
			BufferSize:  1024,
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
	if config.ServerConfig.Address != configData.ServerConfig.Address {
		t.Errorf("Expected ServerConfig.Address %s, got %s", configData.ServerConfig.Address, config.ServerConfig.Address)
	}

	if config.ServerConfig.UseTLS != configData.ServerConfig.UseTLS {
		t.Errorf("Expected ServerConfig.UseTLS %v, got %v", configData.ServerConfig.UseTLS, config.ServerConfig.UseTLS)
	}

	if config.ServerConfig.CertFile != configData.ServerConfig.CertFile {
		t.Errorf("Expected ServerConfig.CertFile %s, got %s", configData.ServerConfig.CertFile, config.ServerConfig.CertFile)
	}

	if config.ServerConfig.KeyFile != configData.ServerConfig.KeyFile {
		t.Errorf("Expected ServerConfig.KeyFile %s, got %s", configData.ServerConfig.KeyFile, config.ServerConfig.KeyFile)
	}

	if config.ServerConfig.ReadTimeout != configData.ServerConfig.ReadTimeout {
		t.Errorf("Expected ServerConfig.ReadTimeout %d, got %d", configData.ServerConfig.ReadTimeout, config.ServerConfig.ReadTimeout)
	}

	if config.ServerConfig.BufferSize != configData.ServerConfig.BufferSize {
		t.Errorf("Expected ServerConfig.BufferSize %d, got %d", configData.ServerConfig.BufferSize, config.ServerConfig.BufferSize)
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
	nr, err := New(logger, "test-key")
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

	defer os.Remove(".journal")
	defer os.Remove(".nodereplica")

	// We create a tcp client to the replica, we know the default port is going to be 4002
	// Resolve the string address to a TCP address
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4002")
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

	// We create a new node replica
	nr, err := New(logger, "test-key")
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

	defer os.Remove(".journal")
	defer os.Remove(".nodereplica")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4002")
	if err != nil {
		nr.Close()
		t.Fatalf("Failed to resolve address: %v", err)
	}

	// Connect to the address with tcp
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

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

	// We create a new node replica
	nr, err := New(logger, "test-key")
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

	defer os.Remove(".journal")
	defer os.Remove(".nodereplica")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4002")
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

	// We create a new node replica
	nr, err := New(logger, "test-key")
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

	defer os.Remove(".journal")
	defer os.Remove(".nodereplica")

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:4002")
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
