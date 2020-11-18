# ☕ kafo-client    [![License](_icon/license.svg)](https://opensource.org/licenses/MIT)

**Kafo-client** 是 kafo 缓存服务的客户端，而 kafo 是一个高性能的轻量级分布式缓存中间件，支持 tcp/http 调用。

### 🖊 使用指南

* 下载依赖：
```bash
go get github.com/avino-plan/kafo-client
```

* 开始编码：
```go
package main

import (
	"github.com/avino-plan/kafo-client"
)

func main() {

	config := kafo.DefaultConfig()
	client, err := kafo.NewTCPClient([]string{"127.0.0.1:5837"}, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// ...
}
```

### ⚛ 性能测试

> R7-4700U，16GB RAM，1000 并发，每个并发 10 个 key，节点数是指 kafo 的进程数

| 操作 | 单节点 | 双节点 | 三节点 | 四节点 | 五节点 |
| --- | ----- | ----- | ----- | ----- | ----- |
| 写入 | 478ms | 263ms | 236ms | 231ms | 230ms |
| 读取 | 475ms | 262ms | 233ms | 226ms | 219ms |
| rps | 21053 | 38168 | 42918 | 44248 | 45662 |

可以看到，随着节点数的提升，性能也在提升，尤其是从单节点变成双节点的时候，性能提升达到了 81%！这是在意料之中的，因为多节点会使用多个 TCP 连接。

但我们也可以看到随着节点数的提升，性能提升的幅度逐渐变小，主要是因为在我的笔记本上测试没办法在网络上达到完全的并行，如果是独立的物理机，相信提升幅度还会更大！

### 🔬 kafo-client 使用的技术

| 项目 | 作者 | 描述 | 链接 |
| -----------|--------|-------------|-------------------|
| vex | FishGoddess | 一个高性能、且极易上手的网络通信框架 | [GitHub](https://github.com/FishGoddess/vex) / [码云](https://gitee.com/FishGoddess/vex) |
