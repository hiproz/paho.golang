package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	// MQTT配置
	mqttServer = "mqtts://device.vkingner.com:50001"
	clientID   = "MQTTBridgeClient"
	downTopic  = "1/down" // 发布主题
	upTopic    = "+/up"   // 订阅主题
	username   = "wd-001"
	password   = "001"

	// HTTP配置
	httpPort      = 50004
	downAPIPath   = "/api/mqtt/down"
	upAPIEndpoint = "http://a.com/api/up"

	// 时间戳验证配置
	maxTimeDiff = 10 * time.Minute

	token    = "123"
	rsp_addr = "devicetest.vkingner.com:50003/api/mqtt/down"
	// req_addr = "http://43.136.89.164:28188/vkapi/iot/device/upstream"
	req_addr = "http://127.0.0.1:50081/api/device/data"
)

var (
	seqCounter int64 = 0
	seqMutex   sync.Mutex
)

// MQTTData 结构体用于解析请求体
type MQTTData struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// MQTTRequest 结构体用于解析整个请求
type MQTTRequest struct {
	MQTTData []MQTTData `json:"mqttdata"`
}

// 验证时间戳
func validateTimestamp(tsStr string) error {
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp format")
	}

	tsTime := time.Unix(ts/1000, (ts%1000)*1000000)
	now := time.Now()
	diff := now.Sub(tsTime)
	if diff < 0 {
		diff = -diff
	}

	if diff > maxTimeDiff {
		return fmt.Errorf("timestamp out of range")
	}

	return nil
}

// 计算token
func calculateToken(token, ts string) string {
	data := token + "+" + ts
	log.Printf("=== Token计算详情 ===")
	log.Printf("输入参数:")
	log.Printf("  token: %s", token)
	log.Printf("  ts: %s", ts)
	log.Printf("拼接结果: %s", data)
	log.Printf("拼接结果字节: %v", []byte(data))

	hash := md5.Sum([]byte(data))
	result := hex.EncodeToString(hash[:])
	log.Printf("MD5计算结果: %s", result)
	log.Printf("MD5字节: %v", hash)
	log.Printf("===================")
	return result
}

// 验证token
func validateToken(inputToken, ts string) error {
	// 计算cal_token = md5sum(token+ts)
	calToken := calculateToken(token, ts) // 使用全局token常量

	// 打印详细的token验证信息
	log.Printf("=== Token验证详情 ===")
	log.Printf("输入的token: %s", inputToken)
	log.Printf("程序token常量: %s", token)
	log.Printf("计算得到的cal_token: %s", calToken)
	log.Printf("验证结果: %v", calToken == inputToken)
	log.Printf("===================")

	// 比较计算得到的cal_token和输入的token
	if calToken != inputToken {
		return fmt.Errorf("invalid token")
	}
	return nil
}

// 验证请求体格式
func validateRequestBody(body []byte) ([]MQTTData, error) {
	var request MQTTRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("invalid json format: %v", err)
	}

	if len(request.MQTTData) == 0 {
		return nil, fmt.Errorf("empty mqtt data array")
	}

	for _, data := range request.MQTTData {
		if data.Topic == "" {
			return nil, fmt.Errorf("missing topic")
		}
		if !json.Valid(data.Payload) {
			return nil, fmt.Errorf("invalid payload json")
		}
	}

	return request.MQTTData, nil
}

// 添加新的结构体用于存储消息
type MessageData struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// 定义一个结构来保存应用状态
type BridgeApp struct {
	mqttConn *autopaho.ConnectionManager
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	msgChan  chan MessageData // 添加消息通道
}

// 检查JSON是否有效
func isValidJSON(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}

// 主线程A: Web服务器处理HTTP请求
func (app *BridgeApp) startWebServer() {
	defer app.wg.Done()

	// 创建HTTP服务器
	mux := http.NewServeMux()

	// 处理下行消息的路由
	mux.HandleFunc(downAPIPath, func(w http.ResponseWriter, r *http.Request) {
		// 记录请求开始
		log.Printf("=== 收到新的HTTP请求 ===")
		log.Printf("请求时间: %s", time.Now().Format("2006-01-02 15:04:05"))
		log.Printf("请求路径: %s", r.URL.Path)
		log.Printf("请求方法: %s", r.Method)
		log.Printf("远程地址: %s", r.RemoteAddr)

		// 检查请求方法
		if r.Method != http.MethodPost {
			sendErrorResponse(w, -1, "仅支持POST请求")
			return
		}

		// 验证必需的请求头
		ts := r.Header.Get("ts")
		seq := r.Header.Get("seq")
		token := r.Header.Get("token")

		if ts == "" || seq == "" || token == "" {
			sendErrorResponse(w, -1, "format error")
			return
		}

		// 读取请求体
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			sendErrorResponse(w, -1, "format error")
			return
		}
		defer r.Body.Close()

		// 打印请求体
		log.Printf("=== 请求体内容 ===")
		log.Printf("%s", string(body))

		// 验证请求体格式
		mqttDataArray, err := validateRequestBody(body)
		if err != nil {
			sendErrorResponse(w, -1, "format error")
			return
		}

		// 发布MQTT消息
		success := true
		for _, data := range mqttDataArray {
			if err := app.publishToMQTT(data.Topic, data.Payload); err != nil {
				log.Printf("发布MQTT消息失败: %v", err)
				success = false
				break
			}
		}

		if success {
			sendSuccessResponse(w)
		} else {
			sendErrorResponse(w, -4, "process failed")
		}
	})

	// 创建HTTP服务器
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: mux,
	}

	// 启动HTTP服务器
	log.Printf("HTTP服务器启动在端口 %d", httpPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP服务器错误: %v", err)
	}
}

// 发布消息到MQTT
func (app *BridgeApp) publishToMQTT(topic string, payload []byte) error {
	log.Printf("正在发布MQTT消息:")
	log.Printf("  主题: %s", topic)
	log.Printf("  QoS: 1")
	log.Printf("  负载: %s", string(payload))

	if app.mqttConn == nil {
		return fmt.Errorf("MQTT连接未建立")
	}

	_, err := app.mqttConn.Publish(app.ctx, &paho.Publish{
		QoS:     1,
		Topic:   topic,
		Payload: payload,
	})

	if err != nil {
		log.Printf("MQTT消息发布失败: %v", err)
	} else {
		log.Printf("MQTT消息发布成功")
	}

	return err
}

// 发送线程C: 订阅MQTT主题并转发到HTTP
func (app *BridgeApp) startMQTTSubscriber() {
	defer app.wg.Done()

	// 创建消息通道
	app.msgChan = make(chan MessageData, 100)

	// 解析服务器URL
	u, err := url.Parse(mqttServer)
	if err != nil {
		log.Printf("解析MQTT服务器URL失败: %v", err)
		return
	}

	// 创建TLS配置
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 禁用证书验证
	}

	// 设置MQTT连接配置
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		TlsCfg:                        tlsConfig,
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		ConnectUsername:               username,
		ConnectPassword:               []byte(password),
		Debug:                         log.New(os.Stdout, "MQTT调试: ", log.Ltime|log.Lshortfile),
		Errors:                        log.New(os.Stderr, "MQTT错误: ", log.Ltime|log.Lshortfile),

		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Println("=== MQTT连接已建立 ===")
			log.Printf("正在订阅主题: %s", upTopic)

			subCtx := context.Background()
			subReq := &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: upTopic, QoS: 1},
				},
			}

			_, err := cm.Subscribe(subCtx, subReq)
			if err != nil {
				log.Printf("订阅主题失败: %v", err)
			} else {
				log.Printf("成功订阅主题: %s", upTopic)
			}
		},

		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					log.Printf("收到消息 - 主题: %s, 负载: %s", pr.Packet.Topic, string(pr.Packet.Payload))

					// 将消息发送到通道
					app.msgChan <- MessageData{
						Topic:   pr.Packet.Topic,
						Payload: pr.Packet.Payload,
					}

					return true, nil
				},
			},
		},
	}

	// 创建MQTT连接
	c, err := autopaho.NewConnection(app.ctx, cliCfg)
	if err != nil {
		log.Printf("创建MQTT连接失败: %v", err)
		return
	}

	app.mqttConn = c

	// 等待连接建立
	if err = c.AwaitConnection(app.ctx); err != nil {
		log.Printf("等待MQTT连接失败: %v", err)
		return
	}

	// 启动消息处理协程
	go app.processMessages()

	// 等待上下文取消
	<-app.ctx.Done()
}

func (app *BridgeApp) processMessages() {
	var messages []MessageData
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg := <-app.msgChan:
			messages = append(messages, msg)
		case <-ticker.C:
			if len(messages) > 0 {
				// 构建请求数据
				requestData := map[string]interface{}{
					"mqttdata": messages,
				}

				// 转换为JSON
				jsonData, err := json.Marshal(requestData)
				if err != nil {
					log.Printf("JSON编码失败: %v", err)
					continue
				}

				// 打印最终JSON
				log.Printf("准备发送的JSON数据: %s", string(jsonData))

				// 发送HTTP请求
				if err := app.sendHTTPRequest(jsonData); err != nil {
					log.Printf("发送HTTP请求失败: %v", err)
				}

				// 清空消息数组
				messages = messages[:0]
			}
		case <-app.ctx.Done():
			return
		}
	}
}

func (app *BridgeApp) sendHTTPRequest(jsonData []byte) error {
	// 创建HTTP请求
	req, err := http.NewRequest("POST", req_addr, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建HTTP请求失败: %v", err)
	}

	// 设置请求头
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	seqMutex.Lock()
	seqCounter++
	seq := strconv.FormatInt(seqCounter, 10)
	seqMutex.Unlock()

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ts", ts)
	req.Header.Set("seq", seq)
	req.Header.Set("rsp_addr", rsp_addr)
	req.Header.Set("token", token)

	// 发送请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送HTTP请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	// 打印响应内容
	log.Printf("HTTP响应状态码: %d", resp.StatusCode)
	log.Printf("HTTP响应内容: %s", string(body))

	return nil
}

func main() {
	// 设置日志
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("MQTT桥接程序启动...")

	// 创建带取消的上下文
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 创建应用实例
	app := &BridgeApp{
		ctx:    ctx,
		cancel: stop,
	}

	// 启动MQTT订阅者
	app.wg.Add(1)
	go app.startMQTTSubscriber()

	// 启动Web服务器
	app.wg.Add(1)
	go app.startWebServer()

	// 等待所有goroutine完成
	app.wg.Wait()

	log.Println("程序已退出")
}

// 检查MQTT服务器端口可用性
func checkServerPort() {
	u, err := url.Parse(mqttServer)
	if err != nil {
		log.Printf("解析MQTT服务器URL失败: %v", err)
		return
	}

	// 从URL中提取主机和端口
	host := u.Host

	// 尝试TCP连接到服务器端口
	log.Printf("正在检查服务器端口可用性: %s", host)
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		log.Printf("警告: 无法连接到MQTT服务器端口: %v", err)
	} else {
		log.Printf("服务器端口检查通过: %s 可达", host)
		conn.Close()
	}
}

// 备用MQTT连接设置 - 尝试不同的配置
func setupAlternativeMQTTConnection(ctx context.Context) (*autopaho.ConnectionManager, error) {
	log.Println("=== 开始设置备用MQTT连接 ===")
	log.Printf("原始MQTT服务器地址: %s", mqttServer)

	// 解析服务器URL - 尝试使用标准MQTT端口1883
	originalURL, _ := url.Parse(mqttServer)
	alternativeURL := fmt.Sprintf("mqtt://%s:1883", originalURL.Hostname())
	u, err := url.Parse(alternativeURL)
	if err != nil {
		log.Printf("解析备用MQTT服务器URL失败: %v", err)
		return nil, fmt.Errorf("解析备用MQTT服务器URL失败: %v", err)
	}

	log.Printf("备用MQTT服务器信息:")
	log.Printf("  完整URL: %s", u.String())
	log.Printf("  Scheme: %s", u.Scheme)
	log.Printf("  Host: %s", u.Host)
	log.Printf("  Path: %s", u.Path)
	log.Printf("  客户端ID: %s", clientID+"-backup")
	log.Printf("  用户名: %s", username)
	log.Printf("  密码长度: %d", len(password))
	log.Printf("  上行主题: %s", upTopic)

	// MQTT连接配置 - 简化版，没有TLS
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		ConnectUsername:               username,
		ConnectPassword:               []byte(password),

		// 添加调试日志
		Debug:  log.New(os.Stdout, "备用MQTT调试: ", log.Ltime|log.Lshortfile),
		Errors: log.New(os.Stderr, "备用MQTT错误: ", log.Ltime|log.Lshortfile),

		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Println("=== 备用MQTT连接已建立 ===")
			log.Printf("连接详情:")
			log.Printf("  服务器: %s", u.String())
			log.Printf("  客户端ID: %s", clientID+"-backup")
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
			log.Printf("正在尝试订阅主题: %s", upTopic)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: upTopic, QoS: 1},
				},
			}); err != nil {
				log.Printf("订阅主题失败 (%s): %v", upTopic, err)
			} else {
				log.Printf("成功订阅主题: %s", upTopic)
			}
			log.Println("=== 备用MQTT连接设置完成 ===")
		},

		OnConnectError: func(err error) {
			log.Printf("=== 备用MQTT连接错误 ===")
			log.Printf("错误详情: %v", err)
			if mqttErr, ok := err.(*autopaho.ConnackError); ok {
				log.Printf("MQTT连接被拒绝:")
				log.Printf("  原因码: %d", mqttErr.ReasonCode)
				log.Printf("  原因: %s", mqttErr.Reason)
			}
			log.Println("===================")
		},

		OnConnectionDown: func() bool {
			log.Printf("=== 备用MQTT连接已断开 ===")
			log.Printf("将尝试重新连接")
			log.Println("===================")
			return true // 返回true表示尝试重新连接
		},
	}

	// 创建连接
	conn, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, fmt.Errorf("创建备用MQTT连接失败: %v", err)
	}

	return conn, nil
}

// 发送错误响应
func sendErrorResponse(w http.ResponseWriter, code int, message string) {
	response := struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// 发送成功响应
func sendSuccessResponse(w http.ResponseWriter) {
	response := struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    0,
		Message: "OK",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
