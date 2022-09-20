package paxos

import (
    "dlog"
    "encoding/binary"
    "fastrpc"
    "genericsmr"
    "genericsmrproto"
    "io"
    "log"
    "math"
    "laproto"
    "state"
    "time"
    "lattice"
)

const CHAN_BUFFER_SIZE = 200000

type Replica struct {
    *genericsmr.Replica   // extends a generic Paxos replica
    proposeChan           chan fastrpc.Serializable
    replyChan	          chan fastrpc.Serializable
    proposeRPC            uint8
    replyRPC	          uint8
    active                bool
    activeProposalNb      uint16
    ackCount              uint16
    nackCount             uint16
    acceptedValue         L
    outputValue           L
    Shutdown              bool
}

func NewReplica(id int, peerAddrList []string, f int) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, false, false, false, false, f),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        0,
        0,
        false,
        -1,
        0,
        0,
        lattice.Bot,
        false,
        }

    r.proposeRPC = r.RegisterRPC(new(laproto.Propose), r.proposeChan)
    r.replyRPC = r.RegisterRPC(new(laproto.Reply), r.replyChan)

    go r.run()

    return r
}

func (r *Replica) replyPropose(replicaId int32, reply *paxosproto.ProposeReply) {
    r.SendMsg(replicaId, r.proposeReplyRPC, reply)
}

func (r *Replica) run() {

    r.ConnectToPeers()
    r.ComputeClosestPeers()
    go r.WaitForClientConnections()

    bcastPropose()

    for !r.Shutdown {

        select {

        case propose := <-proposeChan:
            dlog.Printf("Received a Propose\n") 
            r.handlePropose(propose)
            break

        case reply := <-r.replyChan:
            dlog.Printf("Received reply\n")
            r.handleReply(reply)
            break
        }

    }
}

func (r *Replica) bcastPropose() {
    args := &laproto.Propose{r.Id, r.activeProposalNb, r.acceptedValue}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.proposeRPC, args)
    }
}

func (r *Replica) handlePropose(propose *laproto.Propose) {
    if r.acceptedValue.Leq(propose.Value) {
        r.acceptedValue = propose.Value
    }
    preply := &laproto.ProposeReply{propose.number, diff(r.acceptedValue, propose.Value)}
    r.replyPropose(r.Id, preply)
}

func (r *Replica) handleProposeReply(preply *laproto.ProposeReply) {
    if preply.Number < r.activeProposalNb || r.active == false {
        return
    }

    if preply.Number > r.activeProposalNb {
        dlog.Printf("BUG: This should never happen\n")
        return
    }

    if ProposeReply.Delta.Eq(lattice.Bot) { //ACK
        r.ackCount++
    } else { //NACK
        r.nackCount++
        r.acceptValue = r.acceptValue.Join(preply.Delta)
    }

    if r.ackCount + r.nackCount > (r.N + 1)/2 {
        if r.activeProposalNb > r.F || r.nackCount == 0 { //Decide
            r.active = false
            r.outputValue = r.acceptValue
        } else {
            r.activeProposalNb ++
            r.ackCount = 0
            r.nackCount = 0
            r.bcastPropose()
        }
    }
}
