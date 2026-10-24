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

