package la

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
    "set"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

type Replica struct {
    *genericsmr.Replica   // extends a generic Paxos replica
    viewChan              chan fastrpc.Serializable
    writeChan             chan fastrpc.Serializable
    writeAckChan          chan fastrpc.Serializable
    readChan              chan fastrpc.Serializable
    readAckChan           chan fastrpc.Serializable
    writeRPC              uint8
    writeAckRPC           uint8
    readRPC	          uint8
    readAckRPC	          uint8
    viewRPC	          uint8
    activeProposalNb      uint8
    view                  IntSet
    label                 float32
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

        case view := <-viewChan:
            dlog.Printf("Received a view from replica ...\n") //TODO: extract which replica sent the view and show it here
            r.handleView(view)
            break

        case write := <-writeChan:
            dlog.Printf("Received a write from replica ...\n") //TODO: extract which replica sent the write and show it here
            r.handleWrite(write)
            break

        case writeReply := <-r.writeReplyChan:
            //TODO: extract to which seq the reply was issued and show it here
            dlog.Printf("Received reply for j-th write\n")
            r.handleWrtieReply(writeReply)
            break
        }

        case read := <-readChan:
            dlog.Printf("Received a read from replica ...\n") //TODO: extract which replica sent the write and show it here
            r.handleRead(read)
            break

        case readReply := <-r.readReplyChan:
            //TODO: extract to which seq the reply was issued and show it here
            dlog.Printf("Received reply for j-th read\n")
            r.handleReadReply(readReply)
            break
        }

    }
}

func (r *Replica) bcastView() {
    defer func() {
        if err := recover(); err != nil {
                log.Println("View bcast failed:", err)
        }
    }()

    args := &laproto.View{r.Id, r.view}

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

func (r *Replica) bcastRead() {
    defer func() {
        if err := recover(); err != nil {
                log.Println("Read bcast failed:", err)
        }
    }()

    args := &laproto.Propose{r.Id, r.activeProposalNb, r.proposedValue}

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
    r.acceptedValue = join(r.acceptedValue, propose.Value)
    preply := &laproto.ProposeReply{propose.number, diff(r.acceptedValue, propose.Value}
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
        r.accCount++
    } else { //NACK
        r.nackCount++
        r.proposedValue = join(r.proposedValue, preply.Delta)
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
