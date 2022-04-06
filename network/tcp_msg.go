package network

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// --------------
// | len | data |
// --------------
// Zero Tcp协议 14字节包头
// --------------
// | length | msg_type  | msg_id | crc_32 |  保留      |xxxx xx compression encryption | data |
// |    2  |     2     |    4   |   4    |      1      |          xxxx   xx11         | []byte |
//14
// --------------
type MsgParser struct {
	length      uint16
	msg_type    uint16
	msg_id      uint32
	crc32       uint32
	compression bool
	encryption  bool

	// lenMsgLen    int
	minMsgLen    uint32 //数据最小的字节数
	maxMsgLen    uint32 //数据最大字节数
	littleEndian bool

	INetMempool
}

func NewMsgParser() *MsgParser {
	p := new(MsgParser)
	p.length = 2           //表示消息长度占几个字节
	p.minMsgLen = 1        //数据最小的字节数
	p.maxMsgLen = 4096     //数据最大字节数
	p.littleEndian = false //大端模式
	p.INetMempool = NewMemAreaPool()
	return p
}

// It's dangerous to call the method on reading or writing
func (p *MsgParser) SetMsgLen(lenMsgLen uint16, minMsgLen uint32, maxMsgLen uint32) {
	if lenMsgLen == 1 || lenMsgLen == 2 || lenMsgLen == 4 {
		p.length = lenMsgLen
	}
	if minMsgLen != 0 {
		p.minMsgLen = minMsgLen
	}
	if maxMsgLen != 0 {
		p.maxMsgLen = maxMsgLen
	}

	var max uint32
	switch p.length {
	case 1:
		max = math.MaxUint8
	case 2:
		max = math.MaxUint16
	case 4:
		max = math.MaxUint32
	}
	if p.minMsgLen > max {
		p.minMsgLen = max
	}
	if p.maxMsgLen > max {
		p.maxMsgLen = max
	}
}

// It's dangerous to call the method on reading or writing
func (p *MsgParser) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

//解析对端发过来的数据包是否符合包头标准，符合则返回整个tcp包，包括头部，交由消息处理器处理，否则返回错误信息
// goroutine safe
func (p *MsgParser) Read(conn *TCPConn) ([]byte, error) {
	// 1。获取tcp包长度，14字节包头+可变包体
	buffHeader := make([]byte, 14) //14是包头长度
	_, err := io.ReadFull(conn, buffHeader)
	if err != nil {
		return nil, err
	}
	var datalength uint16
	var crc_32 uint32
	if p.littleEndian {
		datalength = binary.LittleEndian.Uint16(buffHeader[:2])
		crc_32 = binary.LittleEndian.Uint32(buffHeader[8:12])
	} else {
		datalength = binary.BigEndian.Uint16(buffHeader[:2])
		crc_32 = binary.BigEndian.Uint32(buffHeader[8:12])
	}
	// 2.检查长度是否合法
	if uint32(datalength) > p.maxMsgLen {
		return nil, errors.New("message too long")
	} else if uint32(datalength) < p.minMsgLen {
		return nil, errors.New("message too short")
	}

	// 3.拼接头和包
	msgData := p.MakeByteSlice(int(datalength + 14))
	copy(msgData[:14], buffHeader)
	if _, err := io.ReadFull(conn, msgData[14:]); err != nil {
		p.ReleaseByteSlice(msgData)
		return nil, err
	}
	// 4.crc32校验
	if crc32.ChecksumIEEE(msgData[14:]) != crc_32 {
		//校验失败
		return nil, errors.New("crc32 校验失败！")
	}
	//5.返回tcp包
	return msgData, nil

}

// goroutine safe
func (p *MsgParser) Write(conn *TCPConn, args []byte) error {
	// check len
	var length uint16
	if p.littleEndian {
		length = binary.LittleEndian.Uint16(args[:2])
	} else {
		length = binary.BigEndian.Uint16(args[:2])
	}
	if uint32(length) > p.maxMsgLen {
		return errors.New("message too long")
	} else if uint32(length) < p.minMsgLen {
		return errors.New("message too short")
	}

	//  压缩
	if args[13]&0x02 == 1 {

	}
	//加密
	if args[13]&0x01 == 1 {

	}
	conn.Write(args)

	return nil
}
