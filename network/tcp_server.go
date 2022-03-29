package network

import (
	"net"
	"sync"
	"time"

	"github.com/duanhf2012/origin/log"
)

type TCPServer struct {
	Addr            string
	MaxConnNum      int //最大连接数量
	PendingWriteNum int
	NewAgent        func(*TCPConn) Agent //代理
	ln              net.Listener
	conns           ConnSet
	mutexConns      sync.Mutex
	wgLn            sync.WaitGroup
	wgConns         sync.WaitGroup

	// msg parser 消息解析
	LenMsgLen    int    //tcp包最前面表示包长度的位数，默认2
	MinMsgLen    uint32 //最大包长
	MaxMsgLen    uint32 //最小包长
	LittleEndian bool   //是否是小端模式
	msgParser    *MsgParser
	netMemPool   INetMempool
}

func (server *TCPServer) Start() {
	server.init()
	go server.run()
}

func (server *TCPServer) init() {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.SFatal("Listen tcp error:", err.Error())
	}

	if server.MaxConnNum <= 0 {
		server.MaxConnNum = 100
		log.SRelease("invalid MaxConnNum, reset to ", server.MaxConnNum)
	}
	if server.PendingWriteNum <= 0 {
		server.PendingWriteNum = 100
		log.SRelease("invalid PendingWriteNum, reset to ", server.PendingWriteNum)
	}
	if server.NewAgent == nil {
		log.SFatal("NewAgent must not be nil")
	}

	server.ln = ln
	server.conns = make(ConnSet)

	// msg parser
	msgParser := NewMsgParser()
	if msgParser.INetMempool == nil {
		msgParser.INetMempool = NewMemAreaPool()
	}

	msgParser.SetMsgLen(server.LenMsgLen, server.MinMsgLen, server.MaxMsgLen)
	msgParser.SetByteOrder(server.LittleEndian)
	server.msgParser = msgParser
}

func (server *TCPServer) SetNetMempool(mempool INetMempool) {
	server.msgParser.INetMempool = mempool
}

func (server *TCPServer) GetNetMempool() INetMempool {
	return server.msgParser.INetMempool
}

func (server *TCPServer) run() {
	server.wgLn.Add(1)
	defer server.wgLn.Done()

	var tempDelay time.Duration
	for {
		conn, err := server.ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.SRelease("accept error:", err.Error(), "; retrying in ", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return
		}
		conn.(*net.TCPConn).SetNoDelay(true)
		tempDelay = 0

		server.mutexConns.Lock()
		if len(server.conns) >= server.MaxConnNum {
			server.mutexConns.Unlock()
			conn.Close()
			log.SWarning("too many connections")
			continue
		}
		server.conns[conn] = struct{}{}
		server.mutexConns.Unlock()

		server.wgConns.Add(1)

		tcpConn := newTCPConn(conn, server.PendingWriteNum, server.msgParser)
		agent := server.NewAgent(tcpConn)
		go func() {
			agent.Run()

			// cleanup
			tcpConn.Close()
			server.mutexConns.Lock()
			delete(server.conns, conn)
			server.mutexConns.Unlock()
			agent.OnClose()

			server.wgConns.Done()
		}()
	}
}

func (server *TCPServer) Close() {
	server.ln.Close()
	server.wgLn.Wait()

	server.mutexConns.Lock()
	for conn := range server.conns {
		conn.Close()
	}
	server.conns = nil
	server.mutexConns.Unlock()
	server.wgConns.Wait()
}

func (server *TCPServer) GetAddr() string {
	return server.Addr
}
