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
    valueChan             chan fastrpc.Serializable
    writeChan             chan fastrpc.Serializable
    writeReplyChan          chan fastrpc.Serializable
    readChan              chan fastrpc.Serializable
    readReplyChan           chan fastrpc.Serializable
    writeRPC              uint8
    writeReplyRPC           uint8
    readRPC	          uint8
    readReplyRPC	          uint8
    valueRPC	          uint8
    round                 uint8
    view                  IntSet
    label                 float32
    waitWrite             bool
    waitRead              bool
    waitValue             bool
    waitWrite             uint8
    waitRead              uint8
    initValue             L
    outputValue           L
    Shutdown              bool
}

func NewReplica(id int, peerAddrList []string, f int) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, false, false, false, false, f),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
        0,
        0,
        0,
        0,
        0,
        0,
        false,
        -1,
        0,
        0,
        lattice.Bot,
        false,
        }

    r.valueRPC       = r.RegisterRPC(new(laproto.Value), r.valueChan)
    r.writeRPC      = r.RegisterRPC(new(laproto.Write), r.writeChan)
    r.writeReplyRPC = r.RegisterRPC(new(laproto.WriteReply), r.writeReplyChan)
    r.readRPC       = r.RegisterRPC(new(laproto.Read), r.readChan)
    r.readReplyRPC  = r.RegisterRPC(new(laproto.ReadReply), r.readReplyChan)

    go r.run()

    return r
}

func (r *Replica) replyPropose(replicaId int32, reply *paxosproto.ProposeReply) {
    r.SendMsg(replicaId, r.proposeReplyRPC, reply)
}

/* ============= */

/* Main event processing loop */

func (r *Replica) run() {
    r.ConnectToPeers()
    r.ComputeClosestPeers()
    go r.WaitForClientConnections()

    bcastValue()

    for !r.Shutdown {

        select {

        case value := <-valueChan:
            dlog.Printf("Received a value from replica ...\n") //TODO: extract which replica sent the value and show it here
            r.handleValue(value)
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

func (r *Replica) bcastValue() {
    args := &laproto.Value{r.Id, r.initValue}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.valueRPC, args)
    }

    r.view = r.view.join(inputValue)
    r.waitValue = true
    r.valueCount = 1
}

func (r *Replica) bcastRead(round int) {
    args := &laproto.Read{r.Id, round}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.readRPC, args)
        sent++
        if sent >= n {
            break
        }
    }

    r.readReplyVal = r.inputVal.Bot()
    for kj, vj := range r.acceptVal {
        if kj == r.k {
            r.readReplyVal = r.readReplyVal.join(vj)
        }
    }

    r.readCount = 1
    r.waitRead = true
}

func (r *Replica) bcastWrite(v IntSet, k float32, round int) {
    args := &laproto.write{r.Id, v, k, round}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.writeRPC, args)
    }

    r.writeReplyVal = r.inputVal.Bot()
    for kj, vj := range r.acceptVal {
        if kj == r.k {
            r.writeReplyVal = r.writeReplyVal.join(vj)
        }
    }

    r.writeCount = 1
    r.waitWrite = true
}

func (r *Replica) handleValue(value *laproto.Value) {
    if !waitValue {
        return
    }

    r.view = r.view.join(value)
    r.valueCount++

    if r.valueCount == r.N - r.f {
        bcastWrite()
    }
}

func (r *Replica) handleRead(read *laproto.Read) {
    rreply := &laproto.ReadReply{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}

func (r *Replica) handleWrite(write *laproto.Write) {
    acceptVal[write.value][write.label] = true
    wreply := &laproto.WriteReply{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}

func (r *Replica) handleReadReply(rread *laproto.ReadReply) {
    if !waitRead || rread.Round != r.round {
        return
    }

    readReplyVal = readReplyVal.join(
    wreply := &laproto.WriteReply{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}

func (r *Replica) handleWriteReply(rwrite *laproto.WriteReply) {
    acceptVal[write.value][write.label] = true
    wreply := &laproto.WriteReply{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}
