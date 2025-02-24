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
	"log/slog"
	"net"
	"sync"
	"time"
)

// Config is the TCP/TLS server configuration
type Config struct {
	Address     string `yaml:"address"`
	UseTLS      bool   `yaml:"use-tls"`
	CertFile    string `yaml:"cert-file"`
	KeyFile     string `yaml:"key-file"`
	ReadTimeout int    `yaml:"read-timeout"`
	BufferSize  int    `yaml:"buffer-size"`
}

// ConnectionHandler is an interface for handling connections
type ConnectionHandler interface {
	HandleConnection(conn net.Conn)
}

// Server main struct
type Server struct {
	Config     *Config
	Listener   net.Listener
	Wg         sync.WaitGroup
	ShutdownCh chan struct{}
	Logger     *slog.Logger
	ConnCount  int64
	ConnMutex  sync.Mutex
	Handler    ConnectionHandler
}

// New creates a new server
func New(config *Config, logger *slog.Logger, handler ConnectionHandler) *Server {
	return &Server{
		Config:     config,
		ShutdownCh: make(chan struct{}),
		Logger:     logger,
		Handler:    handler,
	}
}

// Start starts the server
func (s *Server) Start() error {
	var listener net.Listener
	var err error

	if s.Config.UseTLS {
		cert, err := tls.LoadX509KeyPair(s.Config.CertFile, s.Config.KeyFile)
		if err != nil {
			return err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		listener, err = tls.Listen("tcp", s.Config.Address, tlsConfig)
	} else {
		listener, err = net.Listen("tcp", s.Config.Address)
	}

	if err != nil {
		return err
	}

	s.Listener = listener
	s.Logger.Info("server started",
		"address", s.Config.Address,
		"tls", s.Config.UseTLS,
	)

	s.acceptConnections()
	return nil
}

// acceptConnections accepts incoming connections
func (s *Server) acceptConnections() {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			select {
			case <-s.ShutdownCh:
				return
			default:
				s.Logger.Error("accept error", "error", err)
				continue
			}
		}

		s.Wg.Add(1)
		s.incrementConnCount()

		go func() {
			defer s.Wg.Done()
			defer s.decrementConnCount()
			s.handleConnection(conn)
		}()
	}
}

// handleConnection handles an incoming connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		_ = conn.Close()
	}(conn)

	// Set connection timeouts
	_ = conn.SetReadDeadline(time.Now().Add(time.Duration(s.Config.ReadTimeout) * time.Second))

	// Use the custom handler to handle the connection
	s.Handler.HandleConnection(conn)
}

// Shutdown shuts down the server
func (s *Server) Shutdown() error {
	close(s.ShutdownCh)

	if s.Listener != nil {
		_ = s.Listener.Close()
	}

	// Wait for all connections to finish with timeout
	done := make(chan struct{})
	go func() {
		s.Wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	}
}

// incrementConnCount increments the connection count
func (s *Server) incrementConnCount() {
	s.ConnMutex.Lock()
	s.ConnCount++
	s.ConnMutex.Unlock()
}

// GetConnCount returns the connection count
func (s *Server) GetConnCount() int64 {
	s.ConnMutex.Lock()
	defer s.ConnMutex.Unlock()
	return s.ConnCount
}

// decrementConnCount decrements the connection count
func (s *Server) decrementConnCount() {
	s.ConnMutex.Lock()
	s.ConnCount--
	s.ConnMutex.Unlock()
}
