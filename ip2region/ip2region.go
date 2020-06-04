package ip2region

import (
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	INDEX_BLOCK_LENGTH  = 12   // 每个索引block的字节数 12byte
	TOTAL_HEADER_LENGTH = 8192 // header区域的长度 8k =》8192byte
)

type IpInfo struct {
	CityId   int64
	Country  string
	Region   string
	Province string
	City     string
	ISP      string
}

type Ip2Region struct {
	// db文件 结构 https://github.com/dongyado/dongyado.github.io/blob/master/_posts/2016-08-18-structure-of-ip2region-database-file.md
	dbFile        string   // db文件路径
	dbFileHandler *os.File // db文件打开后的handler

	dbBinStr []byte // memory 方式查找的时候，用于存所有的数据

	totalBlocks   int64 // 总共索引个数
	firstIndexPtr int64 // 索引的开始位置
	lastIndexPtr  int64 // 索引的结束位置

	headerSip []int64 // header区域中所有的开始ip
	headerPtr []int64 // header区域中所有的数据
	headerLen int64   // header区域block长度
}

func NewIp2Region(filePath string) (*Ip2Region, error) {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Open(filePath); err != nil {
		return nil, err
	}

	return &Ip2Region{
		dbFile:        filePath,
		dbFileHandler: f,
	}, nil
}

func (this *Ip2Region) Close() {
	this.dbFileHandler.Close()
}

func (this *Ip2Region) BtreeSearch(ipStr string) (ipInfo IpInfo, err error) {
	var (
		ipLong  int64
		buffer  []byte
		l       int64
		m       int64
		h       int64
		sptr    int64
		eptr    int64
		dataPtr int64
	)
	ipInfo = IpInfo{}

	if ipLong, err = ip2long(ipStr); err != nil {
		return ipInfo, err
	}

	if this.headerLen == 0 {
		// 声明一个8k长度的切片
		buffer = make([]byte, TOTAL_HEADER_LENGTH)
		// 从文件开始位置偏移8个字节
		this.dbFileHandler.Seek(8, 0)
		// 把数据读到buffer中
		this.dbFileHandler.Read(buffer)

		for i := int64(0); i < TOTAL_HEADER_LENGTH; i += 8 {
			sip := getLong(buffer, i)
			dataPar := getLong(buffer, i+4)

			if dataPar == 0 {
				break
			}
			this.headerSip = append(this.headerSip, sip)
			this.headerPtr = append(this.headerPtr, dataPar)
			this.headerLen++
		}
	}

	// 得到 ipLong所在的范围
	h = this.headerLen
	for l <= h {
		m = (l + h) >> 1
		if m < this.headerLen {
			if ipLong == this.headerSip[m] {
				if m > 0 {
					sptr = this.headerPtr[m-1]
					eptr = this.headerPtr[m]
				} else {
					sptr = this.headerPtr[m]
					eptr = this.headerPtr[m+1]
				}
				break
			}

			if ipLong < this.headerSip[m] {
				if m == 0 {
					sptr = this.headerPtr[m]
					eptr = this.headerPtr[m+1]
					break
				} else if ipLong > this.headerSip[m-1] {
					sptr = this.headerPtr[m-1]
					eptr = this.headerPtr[m]
					break
				}
				h = m - 1
			}

			if ipLong > this.headerSip[m] {
				if m == this.headerLen-1 {
					sptr = this.headerPtr[m-1]
					eptr = this.headerPtr[m]
					break
				} else if ipLong <= this.headerSip[m+1] {
					sptr = this.headerPtr[m]
					eptr = this.headerPtr[m+1]
					break
				}
				l = m + 1
			}

		}
	}

	if sptr == 0 {
		return ipInfo, errors.New("not found")
	}

	blockLen := eptr - sptr
	this.dbFileHandler.Seek(sptr, 0)
	index := make([]byte, blockLen+INDEX_BLOCK_LENGTH)
	this.dbFileHandler.Read(index)

	h = blockLen / INDEX_BLOCK_LENGTH
	l = 0
	for l <= h {
		m = (l + h) >> 1
		p := m * INDEX_BLOCK_LENGTH
		sip := getLong(index, p)
		if ipLong < sip {
			h = m - 1
		} else {
			eip := getLong(index, p+4)
			if ipLong > eip {
				l = m + 1
			} else {
				dataPtr = getLong(index, p+8)
				break
			}
		}
	}

	if dataPtr == 0 {
		return ipInfo, errors.New("not found")
	}

	dataLen := (dataPtr >> 24) & 0xFF
	dataPtr = dataPtr & 0xFFFFFF

	this.dbFileHandler.Seek(dataPtr, 0)
	data := make([]byte, dataLen)
	this.dbFileHandler.Read(data)

	cityId := getLong(data, 0)

	ipInfo = generateIpInfo(cityId, data[4:])

	return
}

func (this *Ip2Region) BinarySearch(ipStr string) (ipInfo IpInfo, err error) {
	ipInfo = IpInfo{}
	if this.totalBlocks == 0 {
		this.dbFileHandler.Seek(0, 0)
		superBlock := make([]byte, 8)
		this.dbFileHandler.Read(superBlock)
		this.firstIndexPtr = getLong(superBlock, 0)
		this.lastIndexPtr = getLong(superBlock, 4)
		this.totalBlocks = (this.lastIndexPtr-this.firstIndexPtr)/INDEX_BLOCK_LENGTH + 1
	}

	var l, dataPtr, p int64

	h := this.totalBlocks

	ip, err := ip2long(ipStr)

	if err != nil {
		return
	}

	for l <= h {
		m := (l + h) >> 1

		p = m * INDEX_BLOCK_LENGTH

		_, err = this.dbFileHandler.Seek(this.firstIndexPtr+p, 0)
		if err != nil {
			return
		}

		buffer := make([]byte, INDEX_BLOCK_LENGTH)
		_, err = this.dbFileHandler.Read(buffer)

		if err != nil {

		}
		sip := getLong(buffer, 0)
		if ip < sip {
			h = m - 1
		} else {
			eip := getLong(buffer, 4)
			if ip > eip {
				l = m + 1
			} else {
				dataPtr = getLong(buffer, 8)
				break
			}
		}

	}

	if dataPtr == 0 {
		err = errors.New("not found")
		return
	}

	dataLen := ((dataPtr >> 24) & 0xFF)
	dataPtr = (dataPtr & 0x00FFFFFF)

	this.dbFileHandler.Seek(dataPtr, 0)
	data := make([]byte, dataLen)
	this.dbFileHandler.Read(data)
	ipInfo = generateIpInfo(getLong(data, 0), data[4:dataLen])
	err = nil
	return
}

func (this *Ip2Region) MemorySearch(ipStr string) (ipInfo IpInfo, err error) {
	var (
		ipLong  int64  // 输入的ip
		l       int64  // 二分查找 索引blocks 开始位置值
		h       int64  // 二分查找 索引blocks 结束位置值
		m       int64  // 二分查找 索引blocks 中间位置值
		pos     int64  // 二分查找 具体查找字节位置
		sip     int64  // 二分查找 ip段开始的ip值
		eip     int64  // 二分查找 ip段结束的ip值
		dataPtr int64  // 二分查找 查找到的值,即:地区信息所在的字节位置
		dataLen int64  // 二分查找 查找到的值的长度,即:数据长度
		cityId  int64  // 城市id  db文件作者项目所需，如用不到忽略即可
		data    []byte // 实际地区数据字节
	)
	ipInfo = IpInfo{} // 返回的值

	// ipStr 转 ipLong
	if ipLong, err = ip2long(ipStr); err != nil {
		return ipInfo, err
	}

	// 获取索引block
	if this.totalBlocks == 0 || len(this.dbBinStr) == 0 {
		// 把数据一次性读入到 dbBinStr 中
		if this.dbBinStr, err = ioutil.ReadFile(this.dbFile); err != nil {
			return ipInfo, err
		}

		this.firstIndexPtr = getLong(this.dbBinStr, 0)
		this.lastIndexPtr = getLong(this.dbBinStr, 4)
		this.totalBlocks = (this.lastIndexPtr-this.firstIndexPtr)/INDEX_BLOCK_LENGTH + 1 // 末尾有部分数据不能形成一个块，所以向上+1
	}

	h = this.totalBlocks
	for l <= this.totalBlocks {
		m = (l + h) >> 1
		pos = this.firstIndexPtr + m*INDEX_BLOCK_LENGTH
		sip = getLong(this.dbBinStr, pos)
		if ipLong < sip {
			// 输入的ip小于ip段的开始ip，则进行下一次查找，查找二分的左边
			h = m - 1
		} else {
			eip = getLong(this.dbBinStr, pos+4)
			if ipLong > eip {
				// 输入ip大于ip段的结束ip，则进行下一次查找，查找二分的右边
				l = m + 1
			} else {
				// 输入的ip存在与该ip段中
				dataPtr = getLong(this.dbBinStr, pos+8)
				break
			}
		}
	}

	if dataPtr == 0 {
		return ipInfo, errors.New("not found")
	}

	// 下面代码看起来似乎是，第一个字节存储的长度，后三个字节存储的数据位置
	// 其实是上文的 getLong 函数在获取数据的时候对字节顺序做了一下反转
	// 数据长度 即:第一个1字节的值
	dataLen = ((dataPtr >> 24) & 0xFF)
	// 数据信息 即：后3字节的值，为实际数据的在整个db中byte的偏移量
	dataPtr = dataPtr & 0x00FFFFFF

	// 前4个字节为城市id
	cityId = getLong(this.dbBinStr, dataPtr)
	// 实际地区数据字节 dataPtr + 4 到 dataPtr+dataLen
	data = this.dbBinStr[dataPtr+4 : dataPtr+dataLen]

	ipInfo = generateIpInfo(cityId, data)

	return
}

// 带符号右移和左移
// >>1 右移一位，二进制向右移动，空位补0。右移一位代表除以2的1次方，右移N位代表除以2的N次方
// <<1 左移一位，二进制向左移动，空位补0。左移一位代表乘以2的1次方，左移N位代表乘以2的N次方

// 获取 当前偏移量中的数据
func getLong(b []byte, offset int64) int64 {
	// 没有为什么， 算法就是这样
	return (int64(b[offset]) |
		int64(b[offset+1])<<8 |
		int64(b[offset+2])<<16 |
		int64(b[offset+3])<<24)
}

// ip转long
func ip2long(ipStr string) (int64, error) {
	bits := strings.Split(ipStr, ".")
	if len(bits) != 4 {
		return 0, errors.New("ip format error, only support ipv4")
	}

	var sum int64
	for i, n := range bits {
		bit, _ := strconv.ParseInt(n, 10, 64)
		sum |= bit << uint(24-8*i)
	}

	return sum, nil
}

// 生成最终的数据
func generateIpInfo(cityId int64, line []byte) IpInfo {
	lineSlice := strings.Split(string(line), "|")
	ipInfo := IpInfo{}
	length := len(lineSlice)
	ipInfo.CityId = cityId
	if length < 5 {
		for i := 0; i <= 5-length; i++ {
			lineSlice = append(lineSlice, "")
		}
	}

	ipInfo.Country = lineSlice[0]
	ipInfo.Region = lineSlice[1]
	ipInfo.Province = lineSlice[2]
	ipInfo.City = lineSlice[3]
	ipInfo.ISP = lineSlice[4]
	return ipInfo
}
