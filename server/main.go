package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
)

// server represents the gRPC server instance.
// It holds the state for all connected clients and the central Lamport clock.
type server struct {
	pb.UnimplementedChitChatServer
	clients      map[string]pb.ChitChat_ChatServer // Maps client IDs to their active gRPC streams
	clientsMutex sync.Mutex                        // Prevents race conditions when adding/removing clients
	lamportClock uint64                            // The server's logical clock counter
	clockMutex   sync.Mutex                        // Prevents race conditions when updating the clock
}

// syncClock applies the Lamport receive rule: L = max(local_L, received_L) + 1.
// It is called every time the server receives a message from a client.
func (s *server) syncClock(receivedTime uint64) uint64 {
	s.clockMutex.Lock()
	defer s.clockMutex.Unlock()
	
	if receivedTime > s.lamportClock {
		s.lamportClock = receivedTime
	}
	s.lamportClock++
	
	return s.lamportClock
}

// incrementClock applies the Lamport internal/send rule: L = L + 1.
// It is called before the server performs a broadcast.
func (s *server) incrementClock() uint64 {
	s.clockMutex.Lock()
	defer s.clockMutex.Unlock()
	
	s.lamportClock++
	return s.lamportClock
}

// broadcast iterates through all active client streams and sends them a message.
func (s *server) broadcast(msg string, excludeClient string) {
	// Lock the clients map so no one can join/leave while we are iterating
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()

	// Increment the server's clock before sending the broadcast out
	sendTime := s.incrementClock()

	for id, stream := range s.clients {
		err := stream.Send(&pb.ServerMessage{
			Message:          msg,
			LamportTimestamp: sendTime,
		})
		
		// Log the success or failure of each individual delivery
		if err != nil {
			log.Printf("Component: Server | Event: Delivery Failed | ClientID: %s | Error: %v", id, err)
		} else {
			log.Printf("Component: Server | Event: Message Delivered | ClientID: %s | Lamport Time: %d", id, sendTime)
		}
	}
}

// Chat is the bidirectional streaming RPC method defined in our .proto file.
// gRPC automatically runs this method in a new, dedicated goroutine for every client that connects.
func (s *server) Chat(stream pb.ChitChat_ChatServer) error {
	var clientID string

	// Infinite loop to continuously listen for incoming messages from this specific client stream
	for {
		in, err := stream.Recv()
		
		// Client gracefully closed the stream
		if err == io.EOF {
			return nil
		}
		
		// An unexpected error occurred (e.g., client crashed or lost network)
		if err != nil {
			// If we already knew who they were, treat this abrupt disconnect as a LEAVE event
			if clientID != "" {
				s.handleLeave(clientID, s.lamportClock)
			}
			return err
		}

		clientID = in.ClientId
		
		// Synchronize the server's clock with the timestamp attached to the incoming message
		newTime := s.syncClock(in.LamportTimestamp)

		// Handle the message based on its EventType enum
		switch in.Event {
		case pb.ClientMessage_JOIN:
			// Safely add the new client stream to the server's active map
			s.clientsMutex.Lock()
			s.clients[clientID] = stream
			s.clientsMutex.Unlock()

			log.Printf("Component: Server | Event: Client Connected | ClientID: %s | Lamport Time: %d", clientID, newTime)
			
			// Format the required join string and broadcast it to everyone
			msg := fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", clientID, newTime)
			s.broadcast(msg, "")

		case pb.ClientMessage_PUBLISH:
			log.Printf("Component: Server | Event: Broadcast Publish | ClientID: %s | Lamport Time: %d", clientID, newTime)
			
			// Format the chat payload and broadcast it to everyone
			msg := fmt.Sprintf("%s: %s", clientID, in.Message)
			s.broadcast(msg, "")

		case pb.ClientMessage_LEAVE:
			s.handleLeave(clientID, newTime)
			return nil // Exiting the function terminates this client's dedicated goroutine
		}
	}
}

// handleLeave cleanly removes a client from the active map and broadcasts their departure.
func (s *server) handleLeave(clientID string, time uint64) {
	// Safely remove the client stream from the map
	s.clientsMutex.Lock()
	delete(s.clients, clientID)
	s.clientsMutex.Unlock()

	log.Printf("Component: Server | Event: Client Disconnected | ClientID: %s | Lamport Time: %d", clientID, time)
	
	// Format the required leave string and broadcast it to the remaining participants
	msg := fmt.Sprintf("Participant %s left Chit Chat at logical time %d", clientID, time)
	s.broadcast(msg, clientID)
}

func main() {
	// Configure the standard logger to output microseconds as per requirements
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Open a TCP listener on port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Component: Server | Event: Startup Failed | Error: %v", err)
	}

	// Initialize the gRPC server instance
	s := grpc.NewServer()
	
	// Register our server implementation with the generated gRPC code
	pb.RegisterChitChatServer(s, &server{
		clients: make(map[string]pb.ChitChat_ChatServer),
	})

	log.Printf("Component: Server | Event: Server Startup | Address: :50051 | Lamport Time: 0")
	
	// Start serving incoming connections (this blocks the main thread)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Component: Server | Event: Shutdown | Error: %v", err)
	}
}
