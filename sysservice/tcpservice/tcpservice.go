package tcpservice

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/duanhf2012/origin/event"
	"github.com/duanhf2012/origin/log"
	"github.com/duanhf2012/origin/network"
	"github.com/duanhf2012/origin/network/processor"
	"github.com/duanhf2012/origin/node"
	"github.com/duanhf2012/origin/service"
)

type TcpService struct {
	tcpServer network.TCPServer
	service.Service

	mapClientLocker sync.RWMutex
	mapClient       map[uint64]*Client
	process         processor.IProcessor

	ReadDeadline  time.Duration
	WriteDeadline time.Duration
	Heartbeat     time.Duration //心跳间隔，暂时用不到，心跳一般客户端发送

}

type TcpPackType int8

const (
	TPT_Connected    TcpPackType = 0
	TPT_DisConnected TcpPackType = 1
	TPT_Pack         TcpPackType = 2
	TPT_UnknownPack  TcpPackType = 3
)

const Default_MaxConnNum = 3000
const Default_PendingWriteNum = 10000
const Default_LittleEndian = false
const Default_MinMsgLen = 2
const Default_MaxMsgLen = 65535
const Default_ReadDeadline = 180  //30s
const Default_WriteDeadline = 180 //30s
const Default_Heartbeat = 15      //心跳包间隔时间15s

const (
	MaxNodeId = 1<<10 - 1 //Uint10
	MaxSeed   = 1<<22 - 1 //MaxUint24
)

var seed uint32
var seedLocker sync.Mutex

type TcpPack struct {
	Type     TcpPackType //0表示连接 1表示断开 2表示数据
	ClientId uint64
	Data     interface{}
}

type Client struct {
	id         uint64
	tcpConn    *network.TCPConn
	tcpService *TcpService
}

//生成客户端id,理论上不会重复
func (tcpService *TcpService) genId() uint64 {
	if node.GetNodeId() > MaxNodeId {
		panic("nodeId exceeds the maximum!")
	}

	seedLocker.Lock()
	seed = (seed + 1) % MaxSeed
	seedLocker.Unlock()

	nowTime := uint64(time.Now().Second())
	return (uint64(node.GetNodeId()) << 54) | (nowTime << 22) | uint64(seed)
}

//获取节点id
func GetNodeId(agentId uint64) int {
	return int(agentId >> 54)
}

//tcpService初始化
func (tcpService *TcpService) OnInit() error {
	//获取配置文件中的配置项
	iConfig := tcpService.GetServiceCfg()
	if iConfig == nil {
		return fmt.Errorf("%s service config is error!", tcpService.GetName())
	}
	tcpCfg := iConfig.(map[string]interface{})
	addr, ok := tcpCfg["ListenAddr"]
	if ok == false {
		return fmt.Errorf("%s service config is error!", tcpService.GetName())
	}

	tcpService.tcpServer.Addr = addr.(string)
	tcpService.tcpServer.MaxConnNum = Default_MaxConnNum
	tcpService.tcpServer.PendingWriteNum = Default_PendingWriteNum
	tcpService.tcpServer.LittleEndian = Default_LittleEndian
	tcpService.tcpServer.MinMsgLen = Default_MinMsgLen
	tcpService.tcpServer.MaxMsgLen = Default_MaxMsgLen
	tcpService.ReadDeadline = Default_ReadDeadline
	tcpService.WriteDeadline = Default_WriteDeadline
	tcpService.Heartbeat = Default_Heartbeat

	MaxConnNum, ok := tcpCfg["MaxConnNum"]
	if ok == true {
		tcpService.tcpServer.MaxConnNum = int(MaxConnNum.(float64))
	}
	PendingWriteNum, ok := tcpCfg["PendingWriteNum"]
	if ok == true {
		tcpService.tcpServer.PendingWriteNum = int(PendingWriteNum.(float64))
	}
	LittleEndian, ok := tcpCfg["LittleEndian"]
	if ok == true {
		tcpService.tcpServer.LittleEndian = LittleEndian.(bool)
	}
	MinMsgLen, ok := tcpCfg["MinMsgLen"]
	if ok == true {
		tcpService.tcpServer.MinMsgLen = uint32(MinMsgLen.(float64))
	}
	MaxMsgLen, ok := tcpCfg["MaxMsgLen"]
	if ok == true {
		tcpService.tcpServer.MaxMsgLen = uint32(MaxMsgLen.(float64))
	}

	readDeadline, ok := tcpCfg["ReadDeadline"]
	if ok == true {
		tcpService.ReadDeadline = time.Second * time.Duration(readDeadline.(float64))
	}

	writeDeadline, ok := tcpCfg["WriteDeadline"]
	if ok == true {
		tcpService.WriteDeadline = time.Second * time.Duration(writeDeadline.(float64))
	}
	//心跳包间隔
	Heartbeat, ok := tcpCfg["Heartbeat"]
	if ok == true {
		tcpService.Heartbeat = time.Second * time.Duration(Heartbeat.(float64))
	}

	tcpService.mapClient = make(map[uint64]*Client, tcpService.tcpServer.MaxConnNum)
	//创建客户端代理，
	tcpService.tcpServer.NewAgent = tcpService.NewClient
	tcpService.tcpServer.Start()

	return nil
}

func (tcpService *TcpService) TcpEventHandler(ev event.IEvent) {
	pack := ev.(*event.Event).Data.(TcpPack)
	switch pack.Type {
	case TPT_Connected:
		tcpService.process.ConnectedRoute(pack.ClientId)
	case TPT_DisConnected:
		tcpService.process.DisConnectedRoute(pack.ClientId)
	case TPT_UnknownPack:
		tcpService.process.UnknownMsgRoute(pack.ClientId, pack.Data)
	case TPT_Pack:
		tcpService.process.MsgRoute(pack.ClientId, pack.Data)
	}
}

//设置tcp消息处理器
func (tcpService *TcpService) SetProcessor(process processor.IProcessor, handler event.IEventHandler) {
	tcpService.process = process
	tcpService.RegEventReceiverFunc(event.Sys_Event_Tcp, handler, tcpService.TcpEventHandler)
}

//新客户端
func (tcpService *TcpService) NewClient(conn *network.TCPConn) network.Agent {
	//加锁
	tcpService.mapClientLocker.Lock()
	//解锁
	defer tcpService.mapClientLocker.Unlock()

	for {
		clientId := tcpService.genId() //生成客户端id，唯一
		_, ok := tcpService.mapClient[clientId]
		if ok == true {
			continue
		}

		pClient := &Client{tcpConn: conn, id: clientId}
		pClient.tcpService = tcpService
		tcpService.mapClient[clientId] = pClient //添加到客户端map中
		return pClient                           //返回新连接的客户端
	}

}

//获取客户端id
func (slf *Client) GetId() uint64 {
	return slf.id
}

// 开始监听客户端
func (slf *Client) Run() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			l := runtime.Stack(buf, false)
			errString := fmt.Sprint(r)
			log.SError("core dump info[", errString, "]\n", string(buf[:l]))
		}
	}()
	//发出tcp连接事件
	slf.tcpService.NotifyEvent(&event.Event{Type: event.Sys_Event_Tcp, Data: TcpPack{ClientId: slf.id, Type: TPT_Connected}})
	for {
		if slf.tcpConn == nil {
			break
		}

		slf.tcpConn.SetReadDeadline(slf.tcpService.ReadDeadline)
		bytes, err := slf.tcpConn.ReadMsg()
		if err != nil {
			log.SDebug("read client id ", slf.id, " is error:", err.Error())
			break
		}
		data, err := slf.tcpService.process.Unmarshal(slf.id, bytes)

		if err != nil {
			//发出tcp接受了未注册的消息类型的事件
			slf.tcpService.NotifyEvent(&event.Event{Type: event.Sys_Event_Tcp, Data: TcpPack{ClientId: slf.id, Type: TPT_UnknownPack, Data: bytes}})
			continue
		}
		//发出tcp接收到数据事件
		slf.tcpService.NotifyEvent(&event.Event{Type: event.Sys_Event_Tcp, Data: TcpPack{ClientId: slf.id, Type: TPT_Pack, Data: data}})
	}
}

//tcp关闭连接触发
func (slf *Client) OnClose() {
	slf.tcpService.NotifyEvent(&event.Event{Type: event.Sys_Event_Tcp, Data: TcpPack{ClientId: slf.id, Type: TPT_DisConnected}})
	slf.tcpService.mapClientLocker.Lock()
	defer slf.tcpService.mapClientLocker.Unlock()
	delete(slf.tcpService.mapClient, slf.GetId()) //从客户端map中删除
}

//发送消息给客户端
func (tcpService *TcpService) SendMsg(clientId uint64, msg interface{}) error {
	tcpService.mapClientLocker.Lock()
	client, ok := tcpService.mapClient[clientId]
	tcpService.mapClientLocker.Unlock()
	if ok == false {
		return fmt.Errorf("client %d is disconnect!", clientId)
	}
	//用设置的tcp消息处理器序列化消息
	bytes, err := tcpService.process.Marshal(clientId, msg)
	if err != nil {
		return err
	}
	//重置tcp写超时时间，超过配置文件的时间没有写入则断开连接
	client.tcpConn.SetWriteDeadline(tcpService.WriteDeadline)
	return client.tcpConn.WriteMsg(bytes)
}

//关闭连接
func (tcpService *TcpService) Close(clientId uint64) {
	tcpService.mapClientLocker.Lock()
	defer tcpService.mapClientLocker.Unlock()
	client, ok := tcpService.mapClient[clientId]
	if !ok {
		return
	}
	if client.tcpConn != nil {
		client.tcpConn.Close()
	}

	return
}

//获取客户端ip地址，获取失败返回空字符串
func (tcpService *TcpService) GetClientIp(clientId uint64) string {
	tcpService.mapClientLocker.Lock()
	defer tcpService.mapClientLocker.Unlock()
	pClient, ok := tcpService.mapClient[clientId]
	if ok == false {
		return ""
	}

	return pClient.tcpConn.GetRemoteIp()
}

//发送[]byte数据，不需要经过消息处理器的序列化。
// Deprecated: 因为命名规范问题函数将废弃,请用SendRawData代替
func (tcpService *TcpService) SendRawMsg(clientId uint64, msg []byte) error {
	tcpService.mapClientLocker.Lock()
	client, ok := tcpService.mapClient[clientId]
	if ok == false {
		tcpService.mapClientLocker.Unlock()
		return fmt.Errorf("client %d is disconnect!", clientId)
	}
	tcpService.mapClientLocker.Unlock()
	client.tcpConn.SetWriteDeadline(tcpService.WriteDeadline)
	return client.tcpConn.WriteMsg(msg)
}

//发送[]byte数据，不需要经过消息处理器的序列化。
func (tcpService *TcpService) SendRawData(clientId uint64, data []byte) error {
	tcpService.mapClientLocker.Lock()
	client, ok := tcpService.mapClient[clientId]
	if ok == false {
		tcpService.mapClientLocker.Unlock()
		return fmt.Errorf("client %d is disconnect!", clientId)
	}
	tcpService.mapClientLocker.Unlock()
	client.tcpConn.SetWriteDeadline(tcpService.WriteDeadline)
	return client.tcpConn.WriteRawMsg(data)
}

func (tcpService *TcpService) GetConnNum() int {
	tcpService.mapClientLocker.Lock()
	connNum := len(tcpService.mapClient)
	tcpService.mapClientLocker.Unlock()
	return connNum
}

//设置网络内存缓存池
func (server *TcpService) SetNetMempool(mempool network.INetMempool) {
	server.tcpServer.SetNetMempool(mempool)
}

func (server *TcpService) GetNetMempool() network.INetMempool {
	return server.tcpServer.GetNetMempool()
}

//释放内存
func (server *TcpService) ReleaseNetMem(byteBuff []byte) {
	server.tcpServer.GetNetMempool().ReleaseByteSlice(byteBuff)
}
