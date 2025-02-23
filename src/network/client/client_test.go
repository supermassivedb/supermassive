// BSD 3-Clause License
//
// (C) Copyright 2025, Alex Gaetano Padula
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
	"net"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	config := &Config{
		ServerAddress:  "localhost:8080",
		UseTLS:         false,
		ConnectTimeout: 5,
		WriteTimeout:   5,
		ReadTimeout:    5,
		MaxRetries:     3,
		RetryWaitTime:  1,
		BufferSize:     1024,
	}
	client := New(config)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestClient_Connect(t *testing.T) {
	config := &Config{
		ServerAddress:  "localhost:8080",
		UseTLS:         false,
		ConnectTimeout: 5,
		WriteTimeout:   5,
		ReadTimeout:    5,
		MaxRetries:     3,
		RetryWaitTime:  1,
		BufferSize:     1024,
	}
	client := New(config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock server
	ln, err := net.Listen("tcp", config.ServerAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}()

	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestClient_Send(t *testing.T) {
	config := &Config{
		ServerAddress:  "localhost:8080",
		UseTLS:         false,
		ConnectTimeout: 5,
		WriteTimeout:   5,
		ReadTimeout:    5,
		MaxRetries:     3,
		RetryWaitTime:  1,
		BufferSize:     1024,
	}
	client := New(config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock server
	ln, err := net.Listen("tcp", config.ServerAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}()

	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = client.Send(ctx, []byte("test data"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestClient_Receive(t *testing.T) {
	config := &Config{
		ServerAddress:  "localhost:8080",
		UseTLS:         false,
		ConnectTimeout: 5,
		WriteTimeout:   5,
		ReadTimeout:    5,
		MaxRetries:     3,
		RetryWaitTime:  1,
		BufferSize:     1024,
	}
	client := New(config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock server
	ln, err := net.Listen("tcp", config.ServerAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.Write([]byte("test data"))
	}()

	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, err := client.Receive(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(data) != "test data" {
		t.Fatalf("expected 'test data', got %s", data)
	}
}

func TestClient_Close(t *testing.T) {
	config := &Config{
		ServerAddress:  "localhost:8080",
		UseTLS:         false,
		ConnectTimeout: 5,
		WriteTimeout:   5,
		ReadTimeout:    5,
		MaxRetries:     3,
		RetryWaitTime:  1,
		BufferSize:     1024,
	}
	client := New(config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock server
	ln, err := net.Listen("tcp", config.ServerAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}()

	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
