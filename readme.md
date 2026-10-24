# Chit Chat - Distributed Chat Service

This repository contains the implementation of "Chit Chat," a distributed chat service built in Go using gRPC. It demonstrates a Client-Server architecture where participants can join, exchange messages, and leave the conversation at any time. The system utilizes Lamport Logical Clocks to synchronize events and ensure causal ordering across all distributed processes.

## Repository Structure
```text
project-root/
├── client/       # Contains the client application code
│   └── main.go 
├── grpc/         # Contains the Protocol Buffers definition and generated code
│   ├── chitchat.proto
│   ├── chitchat.pb.go
│   └── chitchat_grpc.pb.go
├── server/       # Contains the central server application code
│   └── main.go 
├── go.mod        # Go module definitions
├── go.sum        
└── readme.md     # Instructions to run the program

## Running the System
Open three separate terminal windows from the project root and run:

**Terminal 1 (Server):**
```bash
go run server/main.go

Terminal 2 (Client 1):
go run client/main.go -name="Alice"

Terminal 3 (Client 2):
go run client/main.go -name="Bob"

How to Use

    Send a message: Type your text in a client terminal and press Enter.

    Leave the chat: Press Ctrl+C in a client terminal to disconnect and notify others.