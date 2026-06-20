package main

import (
	"disk-scan/api"
	"flag"
	"log"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	addr := flag.String("addr", ":8080", "server listen address")
	flag.Parse()

	server := api.NewServer(*addr)
	log.Printf("Starting disk bad block scan service...")
	log.Printf("Press Ctrl+C to stop")

	if err := server.Run(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
