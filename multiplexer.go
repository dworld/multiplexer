package multiplexer

import (
	"container/heap"
	"errors"
	"io"
	"log"
	"net"
	"time"
)

var (
	ErrTimeout = errors.New("Timeout")
)

type Message interface {
	IsRequest() bool
	IsResponse() bool
	GetSeq() int64
	SetSeq(seq int64)
}

type Parser interface {
	ReadMessage(r io.Reader) (msg Message, err error)
}

type Serialer interface {
	WriteMessage(w io.Writer, msg Message) (err error)
}

type MessageProcessor interface {
	Parser
	Serialer
}

type MessageHandler interface {
	Handle(msg Message)
}

type Multiplexer struct {
	net.Conn

	isClose     bool
	msgp        MessageProcessor
	chanMessage chan Message
	handler     MessageHandler
	maxSeq      int64
	sessions    map[int64]*session
	heap        sessionHeap
}

type session struct {
	req         Message
	respChan    chan interface{}
	beginTime   time.Time
	deadline    time.Time
	seq         int64
	isResponsed bool
}

type sessionHeap []*session

func (h sessionHeap) Len() int           { return len(h) }
func (h sessionHeap) Less(i, j int) bool { return h[i].deadline.UnixNano() < h[j].deadline.UnixNano() }
func (h sessionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h sessionHeap) Push(x interface{}) {
	h = append(h, x.(*session))
}

func (h sessionHeap) Pop() interface{} {
	old := h
	n := len(old)
	x := old[n-1]
	h = old[0 : n-1]
	return x
}

func New(conn net.Conn, processor MessageProcessor, handler MessageHandler) (r *Multiplexer) {
	m := &Multiplexer{Conn: conn, msgp: processor, handler: handler, heap: sessionHeap{}}
	go m.readMessage()
	go m.writeMessage()
	go m.handleTimeout()
	return m
}

func (m *Multiplexer) getNewSeq() int64 {
	for {
		_, ok := m.sessions[m.maxSeq]
		if !ok {
			return m.maxSeq
		}
		m.maxSeq++
	}
}

func (m *Multiplexer) DoRequest(req Message, deadline time.Time) (resp Message, err error) {
	if req.GetSeq() == 0 {
		req.SetSeq(m.getNewSeq())
	}
	session := &session{
		req:       req,
		seq:       req.GetSeq(),
		beginTime: time.Now(),
		deadline:  deadline,
		respChan:  make(chan interface{}),
	}
	// TODO timeout process
	m.sessions[req.GetSeq()] = session
	heap.Push(m.heap, session)
	m.chanMessage <- req
	obj := <-session.respChan
	switch obj.(type) {
	case error:
		err = obj.(error)
	case Message:
		resp = obj.(Message)
	default:
		panic("invalid type")
	}
	return
}

func (m *Multiplexer) SetHandler(h MessageHandler) {
	m.handler = h
	return
}

func (m *Multiplexer) Close() error {
	m.isClose = true
	return m.Conn.Close()
}

func (m *Multiplexer) readMessage() {
	for !m.isClose {
		msg, err := m.msgp.ReadMessage(m.Conn)
		// TODO reconnection
		if err != nil {
			panic(err)
		}
		if msg.IsRequest() {
			go m.handler.Handle(msg)
		}
		if msg.IsResponse() {
			m.handleResponse(msg)
		}
	}
}

func (m *Multiplexer) handleResponse(msg Message) {
	seq := msg.GetSeq()
	session, ok := m.sessions[seq]
	if !ok {
		log.Printf("can't find session[req=%d], may be timeout\n", seq)
		return
	}
	session.isResponsed = true
	session.respChan <- msg
	delete(m.sessions, seq)
}

func (m *Multiplexer) writeMessage() {
	for !m.isClose {
		req := <-m.chanMessage
		err := m.msgp.WriteMessage(m.Conn, req)
		if err != nil {
			panic(err)
		}
	}
}

func (m *Multiplexer) handleTimeout() {
	for {
		if len(m.heap) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		session := heap.Pop(m.heap).(*session)
		now := time.Now()
		deadline := session.deadline
		if now.Before(deadline) {
			time.Sleep(deadline.Sub(now))
		}
		seq := session.seq
		log.Printf("session[req=%d] timeout\n", seq)
		session.respChan <- ErrTimeout
		delete(m.sessions, seq)
	}
}
