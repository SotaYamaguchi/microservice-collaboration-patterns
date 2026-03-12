package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Order は注文エンティティ
type Order struct {
	ID        string
	UserID    string
	ProductID string
	Quantity  int
	Amount    int
	Status    string
	CreatedAt time.Time
}

// OutboxMessage はOutboxテーブルに保存されるメッセージ
// DBトランザクションと同時に保存されることで、At-least-once配信を保証する
type OutboxMessage struct {
	ID          string
	AggregateID string    // 関連するエンティティのID（例: orderID）
	EventType   string    // イベントの種別（例: "OrderCreated"）
	Payload     string    // JSONシリアライズされたイベントデータ
	CreatedAt   time.Time
	ProcessedAt *time.Time // nilの場合は未処理、値がある場合は処理済み
}

// orderCounter は注文IDの連番カウンター
var orderCounter uint64

// generateID は連番IDを生成する
func generateID(prefix string) string {
	n := atomic.AddUint64(&orderCounter, 1)
	return fmt.Sprintf("%s-%d", prefix, n)
}

// InMemoryDB はメモリ上のデータベースをシミュレートする
// 実際のシステムではRDBMSのトランザクションを使用する
type InMemoryDB struct {
	mu       sync.RWMutex
	orders   map[string]*Order
	outbox   []*OutboxMessage
	outboxMu sync.RWMutex
}

// NewInMemoryDB は新しいInMemoryDBを作成する
func NewInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		orders: make(map[string]*Order),
	}
}

// SaveOrderWithOutbox は注文とOutboxメッセージを「トランザクション的に」同時保存する
// これがOutboxパターンの核心: DBへの書き込みとイベント発行の記録を不可分にする
func (db *InMemoryDB) SaveOrderWithOutbox(order *Order, msg *OutboxMessage) error {
	// 実際のDBではここがトランザクション境界になる
	// BEGIN TRANSACTION
	db.mu.Lock()
	db.outboxMu.Lock()
	defer db.mu.Unlock()
	defer db.outboxMu.Unlock()

	db.orders[order.ID] = order

	db.outbox = append(db.outbox, msg)
	// COMMIT

	log.Printf("[InMemoryDB] トランザクション完了: orderID=%s, outboxID=%s", order.ID, msg.ID)
	return nil
}

// GetUnprocessedMessages は未処理のOutboxメッセージを返す（ProcessedAtがnilのもの）
func (db *InMemoryDB) GetUnprocessedMessages() []*OutboxMessage {
	db.outboxMu.RLock()
	defer db.outboxMu.RUnlock()

	var unprocessed []*OutboxMessage
	for _, msg := range db.outbox {
		if msg.ProcessedAt == nil {
			unprocessed = append(unprocessed, msg)
		}
	}
	return unprocessed
}

// MarkAsProcessed はOutboxメッセージを処理済みにする（冪等性のため）
func (db *InMemoryDB) MarkAsProcessed(msgID string) {
	db.outboxMu.Lock()
	defer db.outboxMu.Unlock()

	now := time.Now()
	for _, msg := range db.outbox {
		if msg.ID == msgID {
			msg.ProcessedAt = &now
			log.Printf("[InMemoryDB] メッセージを処理済みにしました: outboxID=%s", msgID)
			return
		}
	}
}

// OrderService は注文サービス
type OrderService struct {
	db *InMemoryDB
}

// NewOrderService は注文サービスを初期化して返す
func NewOrderService(db *InMemoryDB) *OrderService {
	return &OrderService{db: db}
}

// OrderCreatedEventPayload はOrderCreatedイベントのペイロード
type OrderCreatedEventPayload struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Amount    int    `json:"amount"`
}

// SaveOrderWithOutbox は注文を作成し、OutboxメッセージをDBに同時保存する
// これにより、DBへの書き込みとイベント発行の記録が原子的に行われる
func (s *OrderService) SaveOrderWithOutbox(userID, productID string, quantity, amount int) (*Order, error) {
	order := &Order{
		ID:        generateID("order"),
		UserID:    userID,
		ProductID: productID,
		Quantity:  quantity,
		Amount:    amount,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	// イベントペイロードをJSONシリアライズ
	payload := OrderCreatedEventPayload{
		OrderID:   order.ID,
		UserID:    userID,
		ProductID: productID,
		Quantity:  quantity,
		Amount:    amount,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ペイロードのシリアライズに失敗しました: %w", err)
	}

	// OutboxメッセージをOrderと同じトランザクションで保存
	msg := &OutboxMessage{
		ID:          generateID("outbox"),
		AggregateID: order.ID,
		EventType:   "OrderCreated",
		Payload:     string(payloadJSON),
		CreatedAt:   time.Now(),
		ProcessedAt: nil, // 未処理
	}

	if err := s.db.SaveOrderWithOutbox(order, msg); err != nil {
		return nil, fmt.Errorf("注文の保存に失敗しました: %w", err)
	}

	log.Printf("[OrderService] 注文とOutboxメッセージを保存しました: orderID=%s, outboxID=%s",
		order.ID, msg.ID)

	return order, nil
}
