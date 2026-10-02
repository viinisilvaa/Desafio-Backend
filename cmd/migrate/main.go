package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/desafio/wager-service/migrations"
)

func main() {
	direction := "up"
	if len(os.Args) > 1 {
		direction = os.Args[1]
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL, direction); err != nil {
		log.Fatal(err)
	}
}
