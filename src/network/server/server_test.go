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
package server

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

// MockConnectionHandler is a mock implementation of the ConnectionHandler interface
type MockConnectionHandler struct{}

func (m *MockConnectionHandler) HandleConnection(conn net.Conn) {
	// Mock handling connection by writing a response
	defer conn.Close()
	_, _ = conn.Write([]byte("Hello, client :>\n"))
}

// TestServerStart tests the Start method of the Server struct
func TestServerStart(t *testing.T) {
	config := &Config{
		Address:     "localhost:0", // Use port 0 to get an available port
		UseTLS:      false,
		ReadTimeout: 5,
		BufferSize:  1024,
	}

	handler := &MockConnectionHandler{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := New(config, logger, handler)

	// Start the server in a separate goroutine
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual address the server is listening on
	addr := server.Listener.Addr().String()

	// Connect to the server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	_ = conn.Close()

	if err := server.Shutdown(); err != nil {
		t.Errorf("Failed to shutdown server: %v", err)
	}
}

// TestServerShutdown tests the Shutdown method of the Server struct
func TestServerShutdown(t *testing.T) {
	config := &Config{
		Address:     "localhost:0",
		UseTLS:      false,
		ReadTimeout: 5,
		BufferSize:  1024,
	}

	handler := &MockConnectionHandler{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := New(config, logger, handler)

	// Start the server in a separate goroutine
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	if err := server.Shutdown(); err != nil {
		t.Errorf("Failed to shutdown server: %v", err)
	}

	// Ensure the server is no longer accepting connections
	_, err := net.Dial("tcp", server.Listener.Addr().String())
	if err == nil {
		t.Fatalf("Server is still accepting connections after shutdown")
	}
}

// TestServerMultipleConnections tests the server's ability to handle multiple connections
func TestServerMultipleConnections(t *testing.T) {
	config := &Config{
		Address:     "localhost:0",
		UseTLS:      false,
		ReadTimeout: 5,
		BufferSize:  1024,
	}

	handler := &MockConnectionHandler{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := New(config, logger, handler)

	// Start the server in a separate goroutine
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual address the server is listening on
	addr := server.Listener.Addr().String()

	// Connect to the server multiple times
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect to server: %v", err)
		}
		_ = conn.Close()
	}

	if err := server.Shutdown(); err != nil {
		t.Errorf("Failed to shutdown server: %v", err)
	}
}

// Should work on any platform
func generateCerts(certFile, keyFile string) error {
	// Generate private key
	cmd := exec.Command("openssl", "genpkey", "-algorithm", "RSA", "-out", keyFile)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate private key: %v", err)
	}

	// Generate certificate signing request (CSR)
	cmd = exec.Command("openssl", "req", "-new", "-key", keyFile, "-out", "test_cert.csr", "-subj", "/CN=localhost")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate CSR: %v", err)
	}

	// Generate self-signed certificate
	cmd = exec.Command("openssl", "x509", "-req", "-days", "365", "-in", "test_cert.csr", "-signkey", keyFile, "-out", certFile)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate certificate: %v", err)
	}

	// Clean up CSR file
	if err := os.Remove("test_cert.csr"); err != nil {
		return fmt.Errorf("failed to remove CSR file: %v", err)
	}

	return nil
}

// TestServerTLS tests the server's TLS functionality
func TestServerTLS(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	certFile := fmt.Sprintf("%s/test_cert.pem", wd)
	keyFile := fmt.Sprintf("%s/test_key.pem", wd)

	// Generate certificates automatically
	if err := generateCerts(certFile, keyFile); err != nil {
		t.Fatalf("Failed to generate certificates: %v", err)
	}

	// We ensure certificates are removed after test
	defer func() {
		os.Remove(certFile)
		os.Remove(keyFile)
	}()

	config := &Config{
		Address:     "localhost:0",
		UseTLS:      true,
		CertFile:    certFile,
		KeyFile:     keyFile,
		ReadTimeout: 5,
		BufferSize:  1024,
	}

	handler := &MockConnectionHandler{}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := New(config, logger, handler)

	// Start the server in a separate goroutine
	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("Failed to start server: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(512 * time.Millisecond)

	// Get the actual address the server is listening on
	addr := server.Listener.Addr().String()

	// Connect to the server using TLS
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	_ = conn.Close()

	if err := server.Shutdown(); err != nil {
		t.Errorf("Failed to shutdown server: %v", err)
	}
}
