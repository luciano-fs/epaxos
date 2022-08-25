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
const TRUE = uint8(1)
const FALSE = uint8(0)

type Replica struct {
    *genericsmr.Replica   // extends a generic Paxos replica
    proposeChan           chan fastrpc.Serializable
    replyChan	          chan fastrpc.Serializable
    proposeRPC            uint8
    replyRPC	          uint8
    active                bool
    activeProposalNb      uint8
    ackCount              uint8
    nackCount             uint8
    acceptedValue         L
    outputValue           L
    Shutdown              bool
}

func NewReplica(id int, peerAddrList []string, Isleader bool, thrifty bool, exec bool, lread bool, dreply bool, durable bool, f int) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f),
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

    r.Durable = durable

    r.proposeRPC = r.RegisterRPC(new(laproto.Propose), r.proposeChan)
    r.replyRPC = r.RegisterRPC(new(laproto.Reply), r.replyChan)

    go r.run()

    return r
}

//sync with the stable store
func (r *Replica) sync() {
    if !r.Durable {
        return
    }

    r.StableStore.Sync()
}

/* RPC to be called by master */
func (r *Replica) replyPropose(replicaId int32, reply *paxosproto.ProposeReply) {
    r.SendMsg(replicaId, r.proposeReplyRPC, reply)
}

/* ============= */

/* Main event processing loop */

func (r *Replica) run() {

    r.ConnectToPeers()
    r.ComputeClosestPeers()
    go r.WaitForClientConnections()

    for !r.Shutdown {

        select {

        case propose := <-proposeChan:
            dlog.Printf("Received a Propose from replica ...\n") //TODO: extract which replica sent the proposal and show it here
            r.handlePropose(propose)
            break

        case proposeReply := <-r.proposeReplyChan:
            //TODO: check if there needs to be a cast here
            //TODO: extract to which seq the reply was issued and show it here
            dlog.Printf("Received reply for j-th proposal\n")
            r.handleProposeReply(prepareReply)
            break
        }

    }
}

func (r *Replica) bcastPropose() {
    defer func() {
        if err := recover(); err != nil {
                log.Println("Propose bcast failed:", err)
        }
    }()

    args := &laproto.Propose{r.Id, r.activeProposalNb, r.acceptedValue}

    n := r.N - 1

    sent := 0
    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.proposeRPC, args)
        sent++
        if sent >= n {
            break
        }
    }

}

func (r *Replica) handlePropose(propose *laproto.Propose) {
    if leq(r.acceptedValue, propose.Value) {
        r.acceptedValue = propose.Value
    }
    preply := &laproto.ProposeReply{propose.number, diff(r.acceptedValue, propose.Value)}
    r.replyPropose(r.Id, preply)
}

func (r *Replica) handleProposeReply(preply *laproto.ProposeReply) {
    if preply.Number < r.activeProposalNb || r.active == false {
        dlog.Printf("Message in late \n")
        return
    }

    if preply.Number > r.activeProposalNb {
        dlog.Printf("BUG: This should never happen\n")
        return
    }

    if ProposeReply.Delta == lattice.Bot { //ACK
        r.ackCount++
    } else { //NACK
        r.nackCount++
        r.acceptValue = join(r.acceptValue, preply.Delta)
    }

    if r.ackCount + r.nackCount > (r.N + 1)/2 {
        if r.activeProposalNb > r.F || r.nackCount == 0 { //Decide
            r.activeProposalNb ++
            r.ackCount = 0
            r.nackCount = 0
            r.bcastPropose()
        } else {
            r.active = false
            r.outputValue = r.acceptValue
        }
    }
}
