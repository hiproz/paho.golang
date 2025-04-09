package mqttsubtest

import (
	"context"
	"crypto/tls"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	// MQTT配置
	mqttServer = "mqtts://device.vkingner.com:50001"
	clientID   = "MQTTSubTestClient"
	topic      = "x/up" // 订阅主题
	username   = "wd-001"
	password   = "001"
)

func main() {
	// 设置日志
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("MQTT订阅测试程序启动...")

	// 创建带取消的上下文
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 解析服务器URL
	u, err := url.Parse(mqttServer)
	if err != nil {
		log.Fatalf("解析MQTT服务器URL失败: %v", err)
	}

	// 创建TLS配置
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 仅用于测试
	}

	// MQTT连接配置
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		TlsCfg:                        tlsConfig,
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		ConnectUsername:               username,
		ConnectPassword:               []byte(password),

		// 添加调试日志
		Debug:  log.New(os.Stdout, "MQTT调试: ", log.Ltime|log.Lshortfile),
		Errors: log.New(os.Stderr, "MQTT错误: ", log.Ltime|log.Lshortfile),

		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Println("=== MQTT连接已建立 ===")
			log.Printf("连接详情:")
			log.Printf("  服务器: %s", mqttServer)
			log.Printf("  客户端ID: %s", clientID)
			log.Printf("  会话存在: %t", connAck.SessionPresent)
			if connAck.Properties != nil {
				log.Printf("  连接属性:")
				if connAck.Properties.ReasonString != "" {
					log.Printf("    原因: %s", connAck.Properties.ReasonString)
				}
				if connAck.Properties.MaximumQoS != nil {
					log.Printf("    最大QoS: %d", *connAck.Properties.MaximumQoS)
				}
				log.Printf("    保留可用: %t", connAck.Properties.RetainAvailable)
				if connAck.Properties.MaximumPacketSize != nil {
					log.Printf("    最大包大小: %d", *connAck.Properties.MaximumPacketSize)
				}
			}

			// 在连接建立后立即订阅
			log.Printf("正在尝试订阅主题: %s", topic)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: topic, QoS: 1},
				},
			}); err != nil {
				log.Printf("订阅主题失败 (%s): %v", topic, err)
			} else {
				log.Printf("成功订阅主题: %s", topic)
			}
			log.Println("=== MQTT连接设置完成 ===")
		},

		OnConnectError: func(err error) {
			log.Printf("=== MQTT连接错误 ===")
			log.Printf("错误详情: %v", err)
			if mqttErr, ok := err.(*autopaho.ConnackError); ok {
				log.Printf("MQTT连接被拒绝:")
				log.Printf("  原因码: %d", mqttErr.ReasonCode)
				log.Printf("  原因: %s", mqttErr.Reason)
			}
			log.Println("===================")
		},

		OnConnectionDown: func() bool {
			log.Printf("=== MQTT连接已断开 ===")
			log.Printf("将尝试重新连接")
			log.Println("===================")
			return true
		},

		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					log.Printf("=== 收到MQTT消息 ===")
					log.Printf("消息详情:")
					log.Printf("  主题: %s", pr.Packet.Topic)
					log.Printf("  QoS: %d", pr.Packet.QoS)
					log.Printf("  保留标志: %t", pr.Packet.Retain)
					log.Printf("  负载: %s", string(pr.Packet.Payload))
					log.Printf("  消息ID: %d", pr.Packet.PacketID)
					if pr.Packet.Properties != nil {
						log.Printf("  消息属性:")
						if pr.Packet.Properties.ResponseTopic != "" {
							log.Printf("    响应主题: %s", pr.Packet.Properties.ResponseTopic)
						}
						if pr.Packet.Properties.CorrelationData != nil {
							log.Printf("    关联数据: %v", pr.Packet.Properties.CorrelationData)
						}
					}
					log.Println("=== 消息处理完成 ===")
					return true, nil
				},
			},
			OnClientError: func(err error) {
				log.Printf("MQTT客户端错误: %v", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Printf("服务器请求断开连接: %s", d.Properties.ReasonString)
				} else {
					log.Printf("服务器请求断开连接; 原因代码: %d", d.ReasonCode)
				}
			},
		},
	}

	// 创建连接
	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		log.Fatalf("创建MQTT连接失败: %v", err)
	}

	// 等待连接建立
	log.Println("等待MQTT连接建立...")
	if err = c.AwaitConnection(ctx); err != nil {
		log.Fatalf("等待MQTT连接失败: %v", err)
	}
	log.Println("MQTT连接已建立")

	// 等待中断信号
	<-ctx.Done()
	log.Println("收到中断信号，正在关闭...")

	// 关闭连接
	if err := c.Disconnect(context.Background()); err != nil {
		log.Printf("关闭MQTT连接时出错: %v", err)
	}

	log.Println("程序已退出")
}
