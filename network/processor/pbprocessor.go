package processor

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"

	"github.com/duanhf2012/origin/network"
	proto "github.com/golang/protobuf/proto"
)

type MessageInfo struct {
	msgType    reflect.Type
	msgHandler MessageHandler
}

type MessageHandler func(clientId uint64, msg proto.Message)
type ConnectHandler func(clientId uint64)
type UnknownMessageHandler func(clientId uint64, msg []byte)

const MsgTypeSize = 2

type PBProcessor struct {
	mapMsg       map[uint16]MessageInfo
	LittleEndian bool

	unknownMessageHandler UnknownMessageHandler
	connectHandler        ConnectHandler
	disconnectHandler     ConnectHandler
	network.INetMempool
}

type PBPackInfo struct {
	typ    uint16
	msg    proto.Message
	rawMsg []byte
}

func NewPBProcessor() *PBProcessor {
	processor := &PBProcessor{mapMsg: map[uint16]MessageInfo{}}
	processor.INetMempool = network.NewMemAreaPool()
	return processor
}

func (pbProcessor *PBProcessor) SetByteOrder(littleEndian bool) {
	pbProcessor.LittleEndian = littleEndian
}

func (slf *PBPackInfo) GetPackType() uint16 {
	return slf.typ
}

func (slf *PBPackInfo) GetMsg() proto.Message {
	return slf.msg
}

// must goroutine safe
func (pbProcessor *PBProcessor) MsgRoute(clientId uint64, msg interface{}) error {
	pPackInfo := msg.(*PBPackInfo)
	v, ok := pbProcessor.mapMsg[pPackInfo.typ]
	if ok == false {
		return fmt.Errorf("Cannot find msgtype %d is register!", pPackInfo.typ)
	}

	v.msgHandler(clientId, pPackInfo.msg)
	return nil
}

// must goroutine safe
// data 是tcp包头+包体
// --------------
// Zero Tcp协议 14字节包头
// --------------
// | length | msg_type  | msg_id | crc_32 |  保留      |xxxx xx compression encryption | data |
// |    2  |     2     |    4   |   4    |      1      |          xxxx   xx11         | []byte |
//14
// --------------
func (pbProcessor *PBProcessor) Unmarshal(clientId uint64, data []byte) (interface{}, error) {
	defer pbProcessor.ReleaseByteSlice(data)

	// 1. 头
	// var length uint32
	var msgType uint16
	// var msgId uint32
	var compression bool
	var encryption bool

	if pbProcessor.LittleEndian {
		// length = uint32(binary.LittleEndian.Uint16(data[:2]))
		msgType = binary.LittleEndian.Uint16(data[2:4])
		// timestamp = binary.LittleEndian.Uint64(buffHeader[4:12])
		// msgId = binary.LittleEndian.Uint32(data[4:8])
		// crc_32 = binary.LittleEndian.Uint32(data[8:12])

	} else {
		// length = uint32(binary.BigEndian.Uint16(data[:2]))
		msgType = binary.BigEndian.Uint16(data[2:4])
		// timestamp = binary.BigEndian.Uint64(buffHeader[4:12])
		// msgId = binary.BigEndian.Uint32(data[4:8])
		// crc_32 = binary.BigEndian.Uint32(data[8:12])
	}

	compression = data[13]&0x02 != 0
	encryption = data[13]&0x01 != 0

	// 1.从注册的消息类型中查找是否有该类型
	info, ok := pbProcessor.mapMsg[msgType]
	if ok == false {
		return nil, fmt.Errorf("cannot find register %d msgtype!", msgType)
	}

	// 3.解压缩和解密
	//加密了
	if compression {

	}
	//加密了
	if encryption {

	}

	msg := reflect.New(info.msgType.Elem()).Interface()
	protoMsg := msg.(proto.Message)
	err := proto.Unmarshal(data[14:], protoMsg)
	if err != nil {
		return nil, err
	}

	return &PBPackInfo{typ: msgType, msg: protoMsg}, nil
}

// --------------
// Zero Tcp协议 14字节包头
// --------------
// | length | msg_type  | msg_id | crc_32 |  保留      |xxxx xx compression encryption | data |
// |    2  |     2     |    4   |   4    |      1      |          xxxx   xx11         | []byte |
//14
// --------------
// must goroutine safe
// 返回完整tcp包， 包头+包体
func (pbProcessor *PBProcessor) Marshal(clientId uint64, msg interface{}) ([]byte, error) {
	pMsg := msg.(*PBPackInfo)

	var err error
	if pMsg.msg != nil {
		pMsg.rawMsg, err = proto.Marshal(pMsg.msg)
		if err != nil {
			return nil, err
		}
	}

	buff := make([]byte, 14+len(pMsg.rawMsg))
	if pbProcessor.LittleEndian == true {
		binary.LittleEndian.PutUint16(buff[:2], uint16(len(pMsg.rawMsg)))
		binary.LittleEndian.PutUint16(buff[2:4], pMsg.typ)
		binary.LittleEndian.PutUint32(buff[8:12], crc32.ChecksumIEEE(pMsg.rawMsg))

	} else {
		binary.BigEndian.PutUint16(buff[:2], uint16(len(pMsg.rawMsg)))
		binary.BigEndian.PutUint16(buff[2:4], pMsg.typ)
		binary.BigEndian.PutUint32(buff[8:12], crc32.ChecksumIEEE(pMsg.rawMsg))
	}
	buff[13] = 0xf0 //不压缩不加密

	copy(buff[14:], pMsg.rawMsg)
	return buff, nil
	// -----
	// buff = make([]byte, 2, len(pMsg.rawMsg)+MsgTypeSize)
	// if pbProcessor.LittleEndian == true {
	// 	binary.LittleEndian.PutUint16(buff[:2], pMsg.typ)
	// } else {
	// 	binary.BigEndian.PutUint16(buff[:2], pMsg.typ)
	// }

	// buff = append(buff, pMsg.rawMsg...)
	// return buff, nil
}

//msgtype 相当于路由，msg相当于这个tcp包中存储的数据类型，handle是回调函数
//当对端的msgtype是已经注册的类型时，就会调用这个回调函数
func (pbProcessor *PBProcessor) Register(msgtype uint16, msg proto.Message, handle MessageHandler) {
	var info MessageInfo

	info.msgType = reflect.TypeOf(msg.(proto.Message))
	info.msgHandler = handle
	pbProcessor.mapMsg[msgtype] = info
}

func (pbProcessor *PBProcessor) MakeMsg(msgType uint16, protoMsg proto.Message) *PBPackInfo {
	return &PBPackInfo{typ: msgType, msg: protoMsg}
}

func (pbProcessor *PBProcessor) MakeRawMsg(msgType uint16, msg []byte) *PBPackInfo {
	return &PBPackInfo{typ: msgType, rawMsg: msg}
}

func (pbProcessor *PBProcessor) UnknownMsgRoute(clientId uint64, msg interface{}) {
	pbProcessor.unknownMessageHandler(clientId, msg.([]byte))
}

// connect event
func (pbProcessor *PBProcessor) ConnectedRoute(clientId uint64) {
	pbProcessor.connectHandler(clientId)
}

func (pbProcessor *PBProcessor) DisConnectedRoute(clientId uint64) {
	pbProcessor.disconnectHandler(clientId)
}

func (pbProcessor *PBProcessor) RegisterUnknownMsg(unknownMessageHandler UnknownMessageHandler) {
	pbProcessor.unknownMessageHandler = unknownMessageHandler
}

func (pbProcessor *PBProcessor) RegisterConnected(connectHandler ConnectHandler) {
	pbProcessor.connectHandler = connectHandler
}

func (pbProcessor *PBProcessor) RegisterDisConnected(disconnectHandler ConnectHandler) {
	pbProcessor.disconnectHandler = disconnectHandler
}
