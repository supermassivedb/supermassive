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
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"os"
	"time"
)

// Config is the TCP/TLS client configuration
type Config struct {
	ServerAddress  string `yaml:"server-address"`  // The server address the client will connect to
	UseTLS         bool   `yaml:"use-tls"`         // Will the client use TLS?
	CACertFile     string `yaml:"ca-cert-file"`    // The CA cert file to use for TLS
	ConnectTimeout int    `yaml:"connect-timeout"` // If the server doesn't connect in this time, the client will retry
	WriteTimeout   int    `yaml:"write-timeout"`   // Write timeout for connection to server
	ReadTimeout    int    `yaml:"read-timeout"`    // Read timeout for connection to server
	MaxRetries     int    `yaml:"max-retries"`     // Max retries for connection
	RetryWaitTime  int    `yaml:"retry-wait-time"` // Time to wait between retries
	BufferSize     int    `yaml:"buffer-size"`     // Buffer size for reading/writing data, this is the minimum size
}

// Client main struct
type Client struct {
	Config *Config      // Client configuration
	Conn   net.Conn     // Connection to the server
	Logger *slog.Logger // Logger
}

// New creates a new client
func New(config *Config, logger *slog.Logger) *Client {
	return &Client{
		Config: config,
		Logger: logger,
	}
}

// GetConn get's the client server's underlying connection
func (c *Client) GetConn() net.Conn {
	return c.Conn
}

// Connect connects to the server
func (c *Client) Connect(ctx context.Context) error {
	var d net.Dialer
	d.Timeout = time.Duration(c.Config.ConnectTimeout) * time.Second

	var err error
	for i := 0; i <= c.Config.MaxRetries; i++ {
		if i > 0 {
			c.Logger.Info("retrying connection",
				"attempt", i,
				"max_retries", c.Config.MaxRetries,
			)
			time.Sleep(time.Duration(c.Config.RetryWaitTime) * time.Second)
		}

		if c.Config.UseTLS {
			tlsConfig := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}

			// Load CA cert if provided
			if c.Config.CACertFile != "" {
				caCert, err := os.ReadFile(c.Config.CACertFile)
				if err != nil {
					return err
				}
				tlsConfig.RootCAs.AppendCertsFromPEM(caCert)
			}

			c.Conn, err = tls.DialWithDialer(&d, "tcp", c.Config.ServerAddress, tlsConfig)
		} else {
			c.Conn, err = d.DialContext(ctx, "tcp", c.Config.ServerAddress)
		}

		if err == nil {
			c.Logger.Info("connected to server",
				"address", c.Config.ServerAddress,
			)
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return err
}

// Send sends data to the server
func (c *Client) Send(ctx context.Context, data []byte) error {
	if c.Conn == nil {
		return errors.New("not connected")
	}

	// Set write deadline
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(time.Duration(c.Config.WriteTimeout) * time.Second)
	}
	c.Conn.SetWriteDeadline(deadline)

	_, err := c.Conn.Write(data)
	return err
}

// Receive receives data from the server
func (c *Client) Receive(ctx context.Context) ([]byte, error) {
	if c.Conn == nil {
		return nil, errors.New("not connected")
	}

	// Set read deadline
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(time.Duration(c.Config.ReadTimeout) * time.Second)
	}
	c.Conn.SetReadDeadline(deadline)

	buffer := make([]byte, c.Config.BufferSize)
	n, err := c.Conn.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer[:n], nil
}

// Close closes the connection
func (c *Client) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}
