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

type server struct {
	pb.UnimplementedChitChatServer
	clients      map[string]pb.ChitChat_ChatServer
	clientsMutex sync.Mutex
	lamportClock uint64
	clockMutex   sync.Mutex
}

func (s *server) syncClock(receivedTime uint64) uint64 {
	s.clockMutex.Lock()
	defer s.clockMutex.Unlock()
	if receivedTime > s.lamportClock {
		s.lamportClock = receivedTime
	}
	s.lamportClock++
	return s.lamportClock
}

func (s *server) incrementClock() uint64 {
	s.clockMutex.Lock()
	defer s.clockMutex.Unlock()
	s.lamportClock++
	return s.lamportClock
}

func (s *server) broadcast(msg string, excludeClient string) {
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()

	sendTime := s.incrementClock()

	for id, stream := range s.clients {
		err := stream.Send(&pb.ServerMessage{
			Message:          msg,
			LamportTimestamp: sendTime,
		})
		if err != nil {
			log.Printf("Component: Server | Event: Delivery Failed | ClientID: %s | Error: %v", id, err)
		} else {
			log.Printf("Component: Server | Event: Message Delivered | ClientID: %s | Lamport Time: %d", id, sendTime)
		}
	}
}

func (s *server) Chat(stream pb.ChitChat_ChatServer) error {
	var clientID string

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			// If client disconnects abruptly, treat as LEAVE
			if clientID != "" {
				s.handleLeave(clientID, s.lamportClock)
			}
			return err
		}

		clientID = in.ClientId
		newTime := s.syncClock(in.LamportTimestamp)

		switch in.Event {
		case pb.ClientMessage_JOIN:
			s.clientsMutex.Lock()
			s.clients[clientID] = stream
			s.clientsMutex.Unlock()

			log.Printf("Component: Server | Event: Client Connected | ClientID: %s | Lamport Time: %d", clientID, newTime)
			msg := fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", clientID, newTime)
			s.broadcast(msg, "")

		case pb.ClientMessage_PUBLISH:
			log.Printf("Component: Server | Event: Broadcast Publish | ClientID: %s | Lamport Time: %d", clientID, newTime)
			msg := fmt.Sprintf("%s: %s", clientID, in.Message)
			s.broadcast(msg, "")

		case pb.ClientMessage_LEAVE:
			s.handleLeave(clientID, newTime)
			return nil
		}
	}
}

func (s *server) handleLeave(clientID string, time uint64) {
	s.clientsMutex.Lock()
	delete(s.clients, clientID)
	s.clientsMutex.Unlock()

	log.Printf("Component: Server | Event: Client Disconnected | ClientID: %s | Lamport Time: %d", clientID, time)
	msg := fmt.Sprintf("Participant %s left Chit Chat at logical time %d", clientID, time)
	s.broadcast(msg, clientID)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Component: Server | Event: Startup Failed | Error: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterChitChatServer(s, &server{
		clients: make(map[string]pb.ChitChat_ChatServer),
	})

	log.Printf("Component: Server | Event: Server Startup | Address: :50051 | Lamport Time: 0")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Component: Server | Event: Shutdown | Error: %v", err)
	}
}
