package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
	"github.com/streadway/amqp"
)

func main() {
	fmt.Println("RabbitMQ in Golang: Getting started tutorial")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println(err)
	}
	defer connection.Close()

	fmt.Println("Successfully connected to RabbitMQ instance")

	// opening a channel over the connection established to interact with RabbitMQ
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer channel.Close()

	// declaring consumer with its properties over channel opened
	msgs, err := channel.Consume(
		"testing", // queue
		"",        // consumer
		true,      // auto ack
		false,     // exclusive
		false,     // no local
		false,     // no wait
		nil,       //args
	)
	if err != nil {
		log.Println(err)
	}

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", "postgres://root:secret@localhost:5432/rabbit?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	fmt.Println("Successfully connected to PostgreSQL database")

	// Loop to process incoming messages
	for msg := range msgs {
		// Process the message
		fmt.Printf("Received Message: %s\n", msg.Body)
		separator := []byte(",")
		result := bytes.Split(msg.Body, separator)
		// Prepare and execute the insert query for more robust database interaction
		stmt, err := db.Prepare("INSERT INTO t_rabbit(name,age) VALUES ($1,$2)")
		if err != nil {
			log.Printf("Failed to prepare SQL statement: %v", err)
			continue
		}

		defer stmt.Close()

		_, err = stmt.Exec(string(result[0]), string(result[1])) // Use prepared statement with placeholders
		if err != nil {
			log.Printf("Failed to insert data into PostgreSQL: %v", err)
			continue
		}

		fmt.Println("Data inserted into PostgreSQL successfully")
	}

	// print consumed messages from queue
	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			fmt.Printf("Received Message: %s\n", msg.Body)
		}
	}()

	fmt.Println("Waiting for messages...")
	<-forever
}
