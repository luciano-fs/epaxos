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
    replyChan             chan fastrpc.Serializable
    proposeRPC            uint8
    replyRPC              uint8
    active                bool
    activeProposalNb      uint16
    ackCount              uint16
    nackCount             uint16
    proposedValue         L
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
        r.replyRPC = r.RegisterRPC(new(laproto.ProposeReply), r.replyChan)

    go r.run()

    return r
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
            dlog.Printf("Received reply for j-th proposal\n")
            r.handleProposeReply(reply)
            break
        }

    }
}

func (r *Replica) bcastPropose() {
    args := &laproto.Propose{r.Id, r.activeProposalNb, r.proposedValue}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.proposeRPC, args)
    }

}

func (r *Replica) handlePropose(propose *laproto.Propose) {
    r.acceptedValue = r.acceptedValue.Join(propose.Value)
    reply := &laproto.ProposeReply{propose.number, r.acceptedValue.Diff(propose.Value)}
    r.SendMsg(propose.Id, r.replyRPC, reply)
}

func (r *Replica) handleProposeReply(reply *laproto.ProposeReply) {
    if reply.Number < r.activeProposalNb || r.active == false {
        return
    }

    if reply.Number > r.activeProposalNb {
        panic(fmt.sprintf("Received reply to message that was never sent\n")
        return
    }

    if reply.Delta.Eq(r.inputValue.Bot()) { //ACK
        r.accCount++
    } else { //NACK
        r.nackCount++
        r.proposedValue = r.proposedValue.Join(reply.Delta)
    }

    if r.ackCount + r.nackCount > (r.N + 1)/2 {
        if r.nackCount > 0 { //Decide
            r.active = false
            r.outputValue = r.proposedValue
        } else { //Refine
            r.activeProposalNb ++
            r.ackCount = 0
            r.nackCount = 0
            r.bcastPropose()
        }
    }
}
