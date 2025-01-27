package main

import (
	"log"

	"github.com/DistributedClocks/tracing"
)

func main() {
	// Create the tracing server from the provided config file
	tracingServer := tracing.NewTracingServerFromFile("config/tracing-server-config.json")

	// Open the tracing server
	err := tracingServer.Open()
	if err != nil {
		log.Fatalf("Failed to open tracing server: %v\n", err)
	}
	defer tracingServer.Close()

	log.Println("Tracing server is running...")

	// Start accepting connections from clients
	tracingServer.Accept()
}