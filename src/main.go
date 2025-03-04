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
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"supermassive/instance/cluster"
	"supermassive/instance/node"
	"supermassive/instance/nodereplica"
	"syscall"
)

// The system starts here
// You can start SuperMassive as a cluster, node(primary shard) or node-replica(replica of primary shard)
func main() {

	// Variables for the flags
	// They are string pointers..
	instanceTypeFlag := flag.String("instance-type", "cluster", "cluster|node|node-replica")
	sharedKeyFlag := flag.String("shared-key", "", "shared key for cluster to node, node to node replica communication.")

	// If cluster instance
	usernameFlag := flag.String("username", "", "username for client to cluster communication.")
	passwordFlag := flag.String("password", "", "password for client to cluster communication.")

	// Parse the flags
	flag.Parse()

	// When starting an instance
	// If a config file is not in the working directory, a default will be created.
	// cluster has a .cluster which is in yaml format
	// node has a .node which is in yaml format
	// node-replica has a .node-replica which is in yaml format

	// We create a channel for os signals
	sig := make(chan os.Signal, 1)

	// We set that channel to listen for SIGINT and SIGTERM signals
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	// We create a logger for the instance, this gets passed onto internal server and client instances
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Shared key is required for all instances
	if *sharedKeyFlag == "" {
		logger.Error("Shared key is required")
		os.Exit(1)

	}

	// Switch for instance type
	switch *instanceTypeFlag {
	case "cluster":
		logger.Info("Starting cluster instance")

		// Check username and password, this is required for cluster instance
		if *usernameFlag == "" || *passwordFlag == "" {
			logger.Error("Username and password are required for cluster instance")
			os.Exit(1)
		}

		// We cluster, duh
		c, err := cluster.New(logger, *sharedKeyFlag, *usernameFlag, *passwordFlag)
		if err != nil {
			logger.Error("Error creating cluster instance", err)
			os.Exit(1)
		}

		// We use Open method in background as it blocks
		go func() {
			err := c.Open()
			if err != nil {
				logger.Error("Error starting cluster instance", err)
				os.Exit(1)
			}
		}()

		<-sig // We wait for the signal to shutdown

		logger.Info("Shutting down cluster instance")

		// We close the cluster instance
		err = c.Close()
		if err != nil {
			logger.Error("Error shutting down cluster instance", err)
			return
		}

	case "node":
		logger.Info("Starting node instance")

		// We create a node instance
		n, err := node.New(logger, *sharedKeyFlag)
		if err != nil {
			logger.Error("Error creating node instance", err)
			os.Exit(1)
		}

		// We use Open method in background as it blocks
		go func() {
			err := n.Open(nil)
			if err != nil {
				logger.Error("Error starting node instance", err)
				os.Exit(1)
			}
		}()

		<-sig // We wait for the signal to shutdown
		logger.Info("Shutting down node instance")

		// We close the node instance
		err = n.Close()
		if err != nil {
			logger.Error("Error shutting down node instance", err)
			return
		}
	case "node-replica":
		logger.Info("Starting node replica instance")

		// We create a node replica instance
		nr, err := nodereplica.New(logger, *sharedKeyFlag)
		if err != nil {
			logger.Error("Error creating node replica instance", err)
			os.Exit(1)
		}

		// We use Open method in background as it blocks
		go func() {
			err := nr.Open(nil)
			if err != nil {
				logger.Error("Error starting node replica instance", err)
				os.Exit(1)
			}
		}()

		<-sig // We wait for the signal to shutdown
		logger.Info("Shutting down node replica instance")

		// We close the node replica instance
		err = nr.Close()
		if err != nil {
			logger.Error("Error shutting down node replica instance", err)
			return
		}
	default:
		logger.Error("Invalid instance type")
		os.Exit(1)
	}

}
