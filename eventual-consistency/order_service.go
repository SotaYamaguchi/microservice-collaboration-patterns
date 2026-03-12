package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// OrderStatus は注文の状態を表す型
type OrderStatus string

const (
	StatusPending   OrderStatus = "pending"
	StatusConfirmed OrderStatus = "confirmed"
	StatusFailed    OrderStatus = "failed"
)

// Order は注文エンティティ
type Order struct {
	ID        string
	UserID    string
	ProductID string
	Quantity  int
	Status    OrderStatus
	CreatedAt time.Time
}

// OrderCreatedEvent は注文作成イベント
type OrderCreatedEvent struct {
	OrderID   string
	UserID    string
	ProductID string
	Quantity  int
}

// OrderConfirmedEvent は注文確定イベント
type OrderConfirmedEvent struct {
	OrderID string
}

// OrderFailedEvent は注文失敗イベント
type OrderFailedEvent struct {
	OrderID string
	Reason  string
}

// idCounter はIDの連番カウンター
var idCounter uint64

// generateID はシンプルな連番IDを生成する
func generateID() string {
	n := atomic.AddUint64(&idCounter, 1)
	return fmt.Sprintf("order-%d", n)
}

// OrderService は注文サービス
type OrderService struct {
	mu     sync.RWMutex
	orders map[string]*Order

	// イベント発行チャンネル
	OrderCreatedCh chan OrderCreatedEvent

	// イベント受信チャンネル（在庫サービスからの通知）
	OrderConfirmedCh chan OrderConfirmedEvent
	OrderFailedCh    chan OrderFailedEvent
}

// NewOrderService は注文サービスを初期化して返す
func NewOrderService() *OrderService {
	return &OrderService{
		orders:           make(map[string]*Order),
		OrderCreatedCh:   make(chan OrderCreatedEvent, 10),
		OrderConfirmedCh: make(chan OrderConfirmedEvent, 10),
		OrderFailedCh:    make(chan OrderFailedEvent, 10),
	}
}

// CreateOrder は新しい注文を作成し、OrderCreatedEventを発行する
func (s *OrderService) CreateOrder(userID, productID string, quantity int) Order {
	order := Order{
		ID:        generateID(),
		UserID:    userID,
		ProductID: productID,
		Quantity:  quantity,
		Status:    StatusPending, // 初期状態はpending（結果整合性: すぐには確定しない）
		CreatedAt: time.Now(),
	}

	s.mu.Lock()
	s.orders[order.ID] = &order
	s.mu.Unlock()

	log.Printf("[OrderService] 注文を作成しました: orderID=%s, productID=%s, quantity=%d, status=%s",
		order.ID, productID, quantity, order.Status)

	// OrderCreatedEventをチャンネルに非同期で送信
	s.OrderCreatedCh <- OrderCreatedEvent{
		OrderID:   order.ID,
		UserID:    userID,
		ProductID: productID,
		Quantity:  quantity,
	}

	return order
}

// ListenForStatusUpdates は在庫サービスからのイベントを受信して注文状態を更新する
// contextでgoroutineのライフサイクルを管理する
func (s *OrderService) ListenForStatusUpdates(ctx interface{ Done() <-chan struct{} }) {
	log.Println("[OrderService] ステータス更新リスナーを開始します")
	for {
		select {
		case event := <-s.OrderConfirmedCh:
			// 注文確定イベントを受信: 在庫確保成功
			s.updateStatus(event.OrderID, StatusConfirmed)
		case event := <-s.OrderFailedCh:
			// 注文失敗イベントを受信: 在庫不足など
			s.updateStatus(event.OrderID, StatusFailed)
			log.Printf("[OrderService] 注文が失敗しました: orderID=%s, reason=%s", event.OrderID, event.Reason)
		case <-ctx.Done():
			log.Println("[OrderService] ステータス更新リスナーを停止します")
			return
		}
	}
}

// updateStatus は注文のステータスを更新する（内部メソッド）
func (s *OrderService) updateStatus(orderID string, status OrderStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if order, ok := s.orders[orderID]; ok {
		order.Status = status
		log.Printf("[OrderService] 注文状態を更新しました: orderID=%s, status=%s", orderID, status)
	}
}

// GetOrder は注文IDで注文を取得する
func (s *OrderService) GetOrder(orderID string) (Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if order, ok := s.orders[orderID]; ok {
		return *order, true
	}
	return Order{}, false
}
