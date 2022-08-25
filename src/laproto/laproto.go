package paxosproto

import (
    "state"
    "lattice"
)

type Propose struct {
    SenderId   int32
    Number     int32
    Value      lattice.Value
}

type PrepareReply struct {
    Number    int32
    Delta     lattice.Value
}
