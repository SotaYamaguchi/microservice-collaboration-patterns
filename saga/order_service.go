package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// EventType はイベントの種別を表す型
type EventType string

const (
	EventOrderCreated            EventType = "OrderCreated"
	EventPaymentProcessed        EventType = "PaymentProcessed"
	EventPaymentFailed           EventType = "PaymentFailed"
	EventInventoryReserved       EventType = "InventoryReserved"
	EventInventoryReserveFailed  EventType = "InventoryReserveFailed"
	EventOrderCompleted          EventType = "OrderCompleted"
	EventOrderCancelled          EventType = "OrderCancelled"
	EventPaymentRefunded         EventType = "PaymentRefunded"  // 補償トランザクション
	EventInventoryReleased       EventType = "InventoryReleased" // 補償トランザクション
)

// Event はイベントバスを流れるイベントの基底構造体
type Event struct {
	Type    EventType
	Payload interface{}
}

// --- イベントのペイロード定義 ---

type OrderCreatedPayload struct {
	OrderID   string
	UserID    string
	ProductID string
	Quantity  int
	Amount    int
}

type OrderCompletedPayload struct {
	OrderID string
}

type OrderCancelledPayload struct {
	OrderID string
	Reason  string
}

// EventBus はシンプルなpub/subのイベントバス
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[EventType][]chan Event
}

// NewEventBus は新しいEventBusを作成する
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[EventType][]chan Event),
	}
}

// Subscribe は指定したイベント種別の購読チャンネルを登録して返す
func (b *EventBus) Subscribe(eventType EventType) <-chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan Event, 10)
	b.subscribers[eventType] = append(b.subscribers[eventType], ch)
	return ch
}

// Publish は指定したイベントをすべての購読者に送信する
func (b *EventBus) Publish(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Printf("[EventBus] イベント発行: type=%s", event.Type)
	for _, ch := range b.subscribers[event.Type] {
		// ノンブロッキング送信（購読者が遅い場合にデッドロックを防ぐ）
		select {
		case ch <- event:
		default:
			log.Printf("[EventBus] 警告: 購読者のチャンネルが満杯です: type=%s", event.Type)
		}
	}
}

// --- OrderService ---

// OrderStatus は注文状態
type OrderStatus string

const (
	StatusPending   OrderStatus = "pending"
	StatusCompleted OrderStatus = "completed"
	StatusCancelled OrderStatus = "cancelled"
)

// Order は注文エンティティ
type Order struct {
	ID        string
	UserID    string
	ProductID string
	Quantity  int
	Amount    int
	Status    OrderStatus
	CreatedAt time.Time
}

// orderCounter は注文IDの連番カウンター
var orderCounter uint64

// generateOrderID は連番の注文IDを生成する
func generateOrderID() string {
	n := atomic.AddUint64(&orderCounter, 1)
	return fmt.Sprintf("order-%d", n)
}

// OrderService は注文サービス
type OrderService struct {
	mu     sync.RWMutex
	orders map[string]*Order
	bus    *EventBus
}

// NewOrderService は注文サービスを初期化して返す
func NewOrderService(bus *EventBus) *OrderService {
	return &OrderService{
		orders: make(map[string]*Order),
		bus:    bus,
	}
}

// Start はイベントリスナーを起動する（goroutineで実行）
func (s *OrderService) Start(ctx interface{ Done() <-chan struct{} }) {
	// 注文完了イベントを購読
	completedCh := s.bus.Subscribe(EventOrderCompleted)
	// 支払い失敗イベントを購読（Sagaの失敗トリガー）
	paymentFailedCh := s.bus.Subscribe(EventPaymentFailed)
	// 在庫確保失敗イベントを購読（Sagaの失敗トリガー）
	inventoryFailedCh := s.bus.Subscribe(EventInventoryReserveFailed)

	log.Println("[OrderService] イベントリスナーを開始します")
	for {
		select {
		case event := <-completedCh:
			// 注文完了: OrderCompletedイベントを受信して注文を完了状態にする
			payload := event.Payload.(OrderCompletedPayload)
			s.updateStatus(payload.OrderID, StatusCompleted)

		case event := <-paymentFailedCh:
			// 支払い失敗: OrderCancelledを発行してSagaを中止する
			payload := event.Payload.(PaymentFailedPayload)
			s.cancelOrder(payload.OrderID, "支払い失敗")

		case event := <-inventoryFailedCh:
			// 在庫確保失敗: OrderCancelledを発行して補償トランザクションをトリガーする
			payload := event.Payload.(InventoryReserveFailedPayload)
			s.cancelOrder(payload.OrderID, "在庫不足")

		case <-ctx.Done():
			log.Println("[OrderService] イベントリスナーを停止します")
			return
		}
	}
}

// CreateOrder は注文を作成してOrderCreatedイベントを発行する
func (s *OrderService) CreateOrder(userID, productID string, quantity, amount int) string {
	order := Order{
		ID:        generateOrderID(),
		UserID:    userID,
		ProductID: productID,
		Quantity:  quantity,
		Amount:    amount,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}

	s.mu.Lock()
	s.orders[order.ID] = &order
	s.mu.Unlock()

	log.Printf("[OrderService] 注文を作成しました: orderID=%s, userID=%s, productID=%s, amount=%d",
		order.ID, userID, productID, amount)

	// OrderCreatedイベントをSagaの起点として発行
	s.bus.Publish(Event{
		Type: EventOrderCreated,
		Payload: OrderCreatedPayload{
			OrderID:   order.ID,
			UserID:    userID,
			ProductID: productID,
			Quantity:  quantity,
			Amount:    amount,
		},
	})

	return order.ID
}

// cancelOrder は注文をキャンセルしてOrderCancelledイベントを発行する（補償トリガー）
func (s *OrderService) cancelOrder(orderID, reason string) {
	s.updateStatus(orderID, StatusCancelled)
	log.Printf("[OrderService] 注文をキャンセルします: orderID=%s, reason=%s", orderID, reason)

	// OrderCancelledイベントを発行 → 各サービスが補償トランザクションを実行
	s.bus.Publish(Event{
		Type: EventOrderCancelled,
		Payload: OrderCancelledPayload{
			OrderID: orderID,
			Reason:  reason,
		},
	})
}

// updateStatus は注文状態を更新する
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
