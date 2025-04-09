package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const clientID = "PahoGoClient" // 可以改为唯一标识符以避免冲突
const topic = "1/down"          // 指定的主题

func main() {
	// 程序将运行直到用户取消（例如，按下ctrl-c）
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 连接到指定的MQTT服务器
	u, err := url.Parse("mqtts://device.vkingner.com:50001")
	if err != nil {
		panic(err)
	}

	// 创建TLS配置
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 仅用于测试，生产环境请勿使用
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		TlsCfg:     tlsConfig, // 添加TLS配置
		KeepAlive:  20,        // 心跳消息应每20秒发送一次
		// CleanStartOnInitialConnection默认为false。将其设置为true将在首次连接时清除会话。
		CleanStartOnInitialConnection: false,
		// SessionExpiryInterval - 断开连接后会话将存活的秒数。
		SessionExpiryInterval: 60,
		// 添加用户名和密码认证
		ConnectUsername: "wd-001",
		ConnectPassword: []byte("001"),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("mqtt连接已建立")
			// 在OnConnectionUp回调中进行订阅是推荐的做法
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: topic, QoS: 1},
				},
			}); err != nil {
				fmt.Printf("订阅失败 (%s)。这可能意味着将不会收到消息。\n", err)
			} else {
				fmt.Println("已订阅主题:", topic)
			}

			// 连接成功后立即发布"hello"消息
			publishHello(cm, ctx)
		},
		OnConnectError: func(err error) { fmt.Printf("尝试连接时出错: %s\n", err) },
		// eclipse/paho.golang/paho提供基本的mqtt功能，以下配置将用于每个连接
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					fmt.Printf("收到主题%s的消息; 内容: %s (保留: %t)\n", pr.Packet.Topic, pr.Packet.Payload, pr.Packet.Retain)
					return true, nil
				}},
			OnClientError: func(err error) { fmt.Printf("客户端错误: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("服务器请求断开连接: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("服务器请求断开连接; 原因代码: %d\n", d.ReasonCode)
				}
			},
		},
	}

	fmt.Println("正在连接到服务器:", u.String())
	c, err := autopaho.NewConnection(ctx, cliCfg) // 启动进程；将重新连接，直到上下文被取消
	if err != nil {
		panic(err)
	}

	// 等待连接建立
	fmt.Println("等待连接建立...")
	if err = c.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	// 每30秒重新发布一次消息
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			publishHello(c, ctx)
			continue
		case <-ctx.Done():
			fmt.Println("收到信号，准备退出")
			break
		}
		break
	}

	fmt.Println("等待连接清理...")
	<-c.Done() // 等待干净关闭
	fmt.Println("程序已退出")
}

// 发布"hello"消息到指定主题
func publishHello(c *autopaho.ConnectionManager, ctx context.Context) {
	fmt.Printf("正在向主题 %s 发布消息: hello\n", topic)

	if _, err := c.Publish(ctx, &paho.Publish{
		QoS:     1,
		Topic:   topic,
		Payload: []byte("hello"),
	}); err != nil {
		if ctx.Err() == nil {
			fmt.Printf("发布消息失败: %s\n", err)
		}
	} else {
		fmt.Println("消息发布成功")
	}
}
