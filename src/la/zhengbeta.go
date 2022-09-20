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

type Replica struct {
    *genericsmr.Replica   // extends a generic Paxos replica
    valueChan              chan fastrpc.Serializable
    valueAckChan           chan fastrpc.Serializable
    writeChan             chan fastrpc.Serializable
    writeAckChan          chan fastrpc.Serializable
    readChan              chan fastrpc.Serializable
    readAckChan           chan fastrpc.Serializable
    viewRPC               uint8
    viewAckRPC            uint8
    writeRPC              uint8
    writeAckRPC           uint8
    readRPC               uint8
    readAckRPC            uint8
    round                 uint8 
    view                  View
    label                 float32
    waitWrite             bool
    waitRead              bool
    waitValue             bool
    acceptVal             map[uint8][map[float32]View]
    initValue             L
    outputValue           L
    Shutdown              bool
}

func NewReplica(id int, peerAddrList []string, f int) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, false, false, false, false, f),
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

    r.valueRPC     = r.RegisterRPC(new(laproto.Value), r.valueChan)
    r.valueAckRPC  = r.RegisterRPC(new(laproto.ValueAck), r.valueAckChan)
    r.readRPC      = r.RegisterRPC(new(laproto.Read), r.readChan)
    r.readAckRPC   = r.RegisterRPC(new(laproto.ReadAck), r.readAckChan)
    r.writeRPC     = r.RegisterRPC(new(laproto.Write), r.writeChan)
    r.writeAckRPC  = r.RegisterRPC(new(laproto.WriteAck), r.writeAckChan)

    go r.run()

    return r
}

func (r *Replica) run() {
    r.ConnectToPeers()
    r.ComputeClosestPeers()
    go r.WaitForClientConnections()

    bcastValue()

    for !r.Shutdown {

        select {

        case value := <-valueChan:
            dlog.Printf("Received a value from replica ...\n")
            r.handleValue(value)
            break

        case write := <-writeChan:
            dlog.Printf("Received a write from replica ...\n")
            r.handleWrite(write)
            break

        case writeAck := <-r.writeAckChan:
            dlog.Printf("Received reply for j-th write\n")
            r.handleWrtieAck(writeAck)
            break
        }

        case read := <-readChan:
            dlog.Printf("Received a read from replica ...\n")
            r.handleRead(read)
            break

        case readAck := <-r.readAckChan:
            dlog.Printf("Received reply for j-th read\n")
            r.handleReadAck(readAck)
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

    r.view[r.Id] = r.inputValue
    r.waitValue = true
    r.valueCount = 1
}

func (r *Replica) bcastRead(label float32, round int) {
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

    r.readAckVal = r.inputVal.Bot()
    for kj, vj := range r.acceptVal {
        if kj == r.k {
            r.readAckVal = r.readAckVal.join(vj)
        }
    }

    r.readCount = 1
    r.waitRead = true
}

func (r *Replica) bcastWrite(v Set, k float32, round int) {
    args := &laproto.write{r.Id, v, k, round}

    for q := 0; q < r.N-1; q++ {
        if !r.Alive[r.PreferredPeerOrder[q]] {
            continue
        }
        r.SendMsg(r.PreferredPeerOrder[q], r.writeRPC, args)
    }

    r.writeAckVal = r.inputVal.Bot()
    for kj, vj := range r.acceptVal {
        if kj == r.k {
            r.writeAckVal = r.writeAckVal.join(vj)
        }
    }

    r.writeCount = 1
    r.waitWrite = true
}

func (r *Replica) handleValue(value *laproto.Value) {
    if !waitValue {
        return
    }

    r.view = r.view.Join(value)
    r.valueCount++

    if r.valueCount == r.N - r.f {
        bcastWrite()
    }
}

func (r *Replica) handleRead(read *laproto.Read) {
    rreply := &laproto.ReadAck{r.acceptVal,read.Round}
    r.SendMsg(read.Id, r.readAckRPC, rreply)
}

func (r *Replica) handleWrite(write *laproto.Write) {
    acceptVal[write.value][write.label] = true
    wreply := &laproto.WriteAck{r.acceptVal,read.Round}
    r.SendMsg(write.Id, r.writeAckRPC, wreply)
}

func (r *Replica) handleReadAck(rread *laproto.ReadAck) {
    if !waitRead || rread.Round != r.round {
        return
    }

    readAckVal = readAckVal.Join(rread.Value)
    wreply := &laproto.WriteAck{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}

func (r *Replica) handleWriteAck(rwrite *laproto.WriteAck) {
    acceptVal[write.value][write.label] = true
    wreply := &laproto.WriteAck{r.acceptVal,read.Round}
    r.replyRead(r.Id, rreply)
}
