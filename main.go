package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/lzcc1024/goIp2Reg/ip2region"
)

func main() {

	var (
		dbPath string
		ip2reg *ip2region.Ip2Region
		err    error

		reader *bufio.Reader
	)

	// 判断文件是否存在
	dbPath = "./ip2region.db"
	if _, err = os.Stat(dbPath); err != nil {
		fmt.Println(err)
		return
	}

	// 实例化  *ip2region.Ip2Region 对象
	if ip2reg, err = ip2region.NewIp2Region(dbPath); err != nil {
		fmt.Println(err)
		return
	}
	// 关闭文件handler
	defer ip2reg.Close()

	// 定义一个从终端读数据的reader
	reader = bufio.NewReader(os.Stdin)

	for {
		// 198.168.128.118 b-tree,binary,memory
		// 读取输入的数据
		args, _, _ := reader.ReadLine()
		commands := strings.Fields(string(args))
		len := len(commands)
		if len == 0 {
			// 没有任何输入
			continue
		} else if len == 1 {
			// 只输入了ip
			commands = append(commands, "memory")
		}
		if strings.ToLower(commands[0]) == "quit" {
			// 退出
			break
		}

		ipInfo := ip2region.IpInfo{}

		switch commands[1] {
		case "b-tree":
			ipInfo, err = ip2reg.BtreeSearch(commands[0])
		case "binary":
			ipInfo, err = ip2reg.BinarySearch(commands[0])
		case "memory":
			ipInfo, err = ip2reg.MemorySearch(commands[0])
		default:
			err = errors.New("parameter error")
		}

		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(ipInfo)
		}

	}

}
