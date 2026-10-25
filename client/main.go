package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	pb "chitchat/grpc" 

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	lamportClock uint64
	clockMutex   sync.Mutex
	clientID     string
)

// incrementClock increments the local Lamport clock before an internal event or send.
func incrementClock() uint64 {
	clockMutex.Lock()
	defer clockMutex.Unlock()
	lamportClock++
	return lamportClock
}

// syncClock updates the local Lamport clock upon receiving a message.
// Rule: L = max(local_L, received_L) + 1
func syncClock(receivedTime uint64) uint64 {
	clockMutex.Lock()
	defer clockMutex.Unlock()
	if receivedTime > lamportClock {
		lamportClock = receivedTime
	}
	lamportClock++
	return lamportClock
}

func main() {
	// Parse command line arguments for the client's name
	nameFlag := flag.String("name", "Anonymous", "The name of the participant")
	serverAddr := flag.String("server", "localhost:50051", "The server address")
	flag.Parse()
	clientID = *nameFlag

	// Configure standard logger as per requirements
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Connect to the gRPC server
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Component: Client | Event: Connection Failed | ClientID: %s | Error: %v", clientID, err)
	}
	defer conn.Close()

	client := pb.NewChitChatClient(conn)

	// Open the bidirectional stream
	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("Component: Client | Event: Stream Initialization Failed | ClientID: %s | Error: %v", clientID, err)
	}

	// 1. Send JOIN message
	joinTime := incrementClock()
	err = stream.Send(&pb.ClientMessage{
		ClientId:         clientID,
		Event:            pb.ClientMessage_JOIN,
		LamportTimestamp: joinTime,
	})
	if err != nil {
		log.Fatalf("Component: Client | Event: Join Failed | ClientID: %s | Error: %v", clientID, err)
	}
	log.Printf("Component: Client | Event: Sent Join | ClientID: %s | Lamport Time: %d", clientID, joinTime)

	// 2. Start concurrent goroutine for receiving messages
	go receiveMessages(stream)

	// 3. Start concurrent goroutine for reading standard input and sending messages
	go handleUserInput(stream)

	// 4. Handle graceful shutdown (Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh // Block main thread until a signal is received

	// Send LEAVE message upon exit
	leaveTime := incrementClock()
	err = stream.Send(&pb.ClientMessage{
		ClientId:         clientID,
		Event:            pb.ClientMessage_LEAVE,
		LamportTimestamp: leaveTime,
	})
	if err != nil {
		log.Printf("Component: Client | Event: Leave Failed | ClientID: %s | Error: %v", clientID, err)
	} else {
		log.Printf("Component: Client | Event: Sent Leave | ClientID: %s | Lamport Time: %d", clientID, leaveTime)
	}

	// Close the send direction of the stream
	stream.CloseSend()
	fmt.Println("\nDisconnected from Chit Chat.")
}

// receiveMessages continuously listens for server broadcasts.
func receiveMessages(stream pb.ChitChat_ChatClient) {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Component: Client | Event: Stream Disconnected | ClientID: %s | Error: %v", clientID, err)
			return
		}

		// Update Lamport clock on receive
		newTime := syncClock(in.LamportTimestamp)

		// Log the delivery event as required
		log.Printf("Component: Client | Event: Message Delivered | ClientID: %s | Lamport Time: %d | Msg: %s",
			clientID, newTime, in.Message)

		// Display the message on the client interface
		fmt.Printf("\n[%d] %s\n> ", newTime, in.Message)
	}
}

// handleUserInput reads from terminal and publishes messages.
func handleUserInput(stream pb.ChitChat_ChatClient) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := scanner.Text()
		text = strings.TrimSpace(text)

		if text == "" {
			fmt.Print("> ")
			continue
		}

		// Requirement: Validate max 128 characters and UTF-8 encoding
		if !utf8.ValidString(text) {
			fmt.Println("Error: Message must be valid UTF-8 string.")
			fmt.Print("> ")
			continue
		}
		if utf8.RuneCountInString(text) > 128 {
			fmt.Println("Error: Message exceeds maximum length of 128 characters.")
			fmt.Print("> ")
			continue
		}

		// Increment clock before publishing
		pubTime := incrementClock()

		err := stream.Send(&pb.ClientMessage{
			ClientId:         clientID,
			Message:          text,
			Event:            pb.ClientMessage_PUBLISH,
			LamportTimestamp: pubTime,
		})

		if err != nil {
			log.Printf("Component: Client | Event: Publish Failed | ClientID: %s | Error: %v", clientID, err)
			return
		}

		log.Printf("Component: Client | Event: Sent Publish | ClientID: %s | Lamport Time: %d", clientID, pubTime)

		// Reprint the prompt cursor with a slight delay to prevent prompt clashing with incoming broadcasts
		time.Sleep(50 * time.Millisecond)
		fmt.Print("> ")
	}
}
