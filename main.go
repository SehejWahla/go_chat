package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	server, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Initialize(); err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	router := mux.NewRouter()
	server.RegisterRoutes(router)

	httpServer := &http.Server{
		Addr:    ":" + os.Getenv("PORT"),
		Handler: router,
	}
	if httpServer.Addr == ":" {
		httpServer.Addr = ":8080"
	}

	go func() {
		log.Printf("Starting server on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	if err := server.kafkaClient.StartConsumers(server); err != nil {
		log.Fatalf("Failed to start Kafka consumers: %v", err)
	}

	go server.StartBatchProcessor()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	server.Shutdown()
	log.Println("Server shutdown complete")
}
