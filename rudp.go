/*
 Copyright 2015 Bluek404

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package main

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

func main() {
	conn, err := NewConn("udp", &net.UDPAddr{
		IP:   net.ParseIP("localhost"),
		Port: 8082,
	})
	if err != nil {
		log.Println(err)
		return
	}
	data := make([]byte, 10241)
	n, err := conn.WriteToUDP(data, &net.UDPAddr{
		IP:   net.ParseIP("localhost"),
		Port: 8082,
	})
	if err != nil {
		log.Println(n, err)
	}
	time.Sleep(time.Second * 10)
}

var (
	BufSize = 512
	rnd     = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func toBlockID(in []byte) int {
	out, err := strconv.Atoi(string(in))
	if err != nil {
		log.Println(err)
		return -1
	}
	return out
}

func NewConn(netType string, laddr *net.UDPAddr) (*RUDPConn, error) {
	udpConn, err := net.ListenUDP(netType, laddr)
	if err != nil {
		return nil, err
	}
	conn := &RUDPConn{
		UDPConn: *udpConn,
		buffer:  make(map[string]dataBuf),
	}
	go conn.readConn()
	return conn, err
}

// Reliability UDP
type RUDPConn struct {
	net.UDPConn

	buffer map[string]dataBuf
	closed bool
}

func (r *RUDPConn) WriteToUDP(data []byte, addr *net.UDPAddr) (int, error) {
	dataSize := len(data)
	// 将文件分为多少块
	blockNum := dataSize / BufSize
	// 最后的文件块大小
	lastBlockSize := dataSize - blockNum*BufSize
	if lastBlockSize > 0 {
		blockNum++
	}
	// 用于记录各个文件块的状态
	//blockStatus := make([]bool, blockNum)

	// 文件ID，用于标识文件
	dataID := []byte(strconv.FormatInt(rnd.Int63(), 36))
	dataIDLen := len(dataID)

	var (
		n          int
		err        error
		writeBlock = func(i int) (int, error) {
			begin := i * BufSize
			end := (i + 1) * BufSize
			if end > dataSize {
				end = begin + lastBlockSize
			}

			blockID := []byte(strconv.Itoa(i))
			// 计算文件块ID所占用的byte大小
			blockIDLen := len(blockID)

			// 生成文件块,结构为：
			// DataID的大小（1byte）+DataID（不固定）+BlockID的大小（1byte）+BlockID（不固定）+数据（<=512byte）
			head := append(append([]byte{byte(dataIDLen)}, dataID...), append([]byte{byte(blockIDLen)}, blockID...)...)
			block := append(head, data[begin:end]...)

			nn, e := r.UDPConn.WriteToUDP(block, addr)
			if e != nil {
				return 0, e
			}
			return nn, nil
		}
	)

	// 发送文件信息
	// 0+DataID大小+DataID+文件块数量
	blockInfo := append(append([]byte{0, byte(dataIDLen)}, dataID...), []byte(strconv.Itoa(blockNum))...)
	nn, e := r.UDPConn.WriteToUDP(blockInfo, addr)
	if e != nil {
		return nn, e
	}
	for i := 0; i < blockNum; i++ {
		nn, e := writeBlock(i)
		if e != nil && err == nil {
			err = e
		}
		n += nn
	}
	// TODO: 丢包重传
	return n, err
}

// 后台接收处理包
// 根据ID组装文件
func (r *RUDPConn) readConn() {
	var buf = make([]byte, BufSize+256)
	for !r.closed {
		n, addr, err := r.UDPConn.ReadFrom(buf)
		if err != nil {
			println(err.Error())
		}
		if buf[0] == 0 {
			// 新文件
			dataID := string(buf[2 : buf[1]+2])
			dataSize := toBlockID(buf[buf[1]+2 : n])
			if dataSize == -1 {
				continue
			}
			r.buffer[addr.String()+dataID] = dataBuf{
				blocks: make([][]byte, dataSize),
			}
			log.Println("new data!!", dataID, dataSize)
			continue
		}
		dataID := string(buf[1 : buf[0]+1])
		if v, ok := r.buffer[addr.String()+dataID]; ok {
			dataIDLen := len(buf[1 : buf[0]+1])
			begin := dataIDLen + 2
			end := begin + int(buf[dataIDLen+1])
			blockID := toBlockID(buf[begin:end])
			data := buf[dataIDLen+end-begin+2 : n]
			v.blocks[blockID] = data
			log.Println(dataID, blockID, n, addr, len(data))
			// TODO: 判断文件的完整性，以及丢包重传
		}
		// 不存在此文件，丢弃包
	}
}

func (r *RUDPConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	// TODO: 使用chan从readConn接收文件
	// dataChan->readChan
	// data:=<-dataChan
	return r.UDPConn.ReadFromUDP(b)
}

func (r *RUDPConn) Close() error {
	err := r.UDPConn.Close()
	if err != nil {
		return err
	}
	r.closed = true
	return nil
}

type dataBuf struct {
	blocks [][]byte
	data   []byte
}
