package main

import (
	"log"
	"sync"
)

// InventoryReservedPayload は在庫確保完了イベントのペイロード
type InventoryReservedPayload struct {
	OrderID string
}

// InventoryReserveFailedPayload は在庫確保失敗イベントのペイロード
type InventoryReserveFailedPayload struct {
	OrderID string
	Reason  string
}

// InventoryReleasedPayload は在庫解放イベントのペイロード（補償トランザクション）
type InventoryReleasedPayload struct {
	OrderID string
}

// InventoryService は在庫サービス
// PaymentProcessedイベントを受信して在庫確保を行い、結果をイベントとして発行する
type InventoryService struct {
	mu        sync.Mutex
	inventory map[string]int // productID -> 在庫数

	// 確保済み在庫の記録（補償トランザクションで解放するため）
	reserved map[string]reservedItem // orderID -> 確保情報

	bus *EventBus
}

// reservedItem は確保済み在庫の情報
type reservedItem struct {
	ProductID string
	Quantity  int
}

// NewInventoryService は在庫サービスを初期化して返す
func NewInventoryService(bus *EventBus) *InventoryService {
	return &InventoryService{
		inventory: map[string]int{
			"product-A": 10,
			"product-B": 5,
			"product-C": 0,
		},
		reserved: make(map[string]reservedItem),
		bus:      bus,
	}
}

// Start はイベントリスナーを起動する（goroutineで実行）
func (s *InventoryService) Start(ctx interface{ Done() <-chan struct{} }) {
	// 支払い完了イベントを購読（Sagaの第2ステップ: 在庫確保）
	paymentProcessedCh := s.bus.Subscribe(EventPaymentProcessed)
	// 注文キャンセルイベントを購読（補償トランザクションのトリガー）
	orderCancelledCh := s.bus.Subscribe(EventOrderCancelled)

	log.Println("[InventoryService] イベントリスナーを開始します")
	for {
		select {
		case event := <-paymentProcessedCh:
			// 支払い完了イベントを受信: 在庫確保を実行
			payload := event.Payload.(PaymentProcessedPayload)
			s.reserveInventory(payload)

		case event := <-orderCancelledCh:
			// 注文キャンセルイベントを受信: 補償トランザクション（在庫解放）を実行
			payload := event.Payload.(OrderCancelledPayload)
			s.releaseInventory(payload.OrderID)

		case <-ctx.Done():
			log.Println("[InventoryService] イベントリスナーを停止します")
			return
		}
	}
}

// reserveInventory は在庫確保を試みる
// 成功時はInventoryReservedイベント → OrderCompletedイベントを発行
// 失敗時はInventoryReserveFailedイベントを発行（補償トランザクションをトリガー）
func (s *InventoryService) reserveInventory(payload PaymentProcessedPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[InventoryService] 在庫確保を試みます: orderID=%s, productID=%s, quantity=%d",
		payload.OrderID, payload.ProductID, payload.Quantity)

	stock, exists := s.inventory[payload.ProductID]
	if !exists || stock < payload.Quantity {
		// 在庫不足: InventoryReserveFailedイベントを発行
		// → OrderServiceがOrderCancelledを発行し補償トランザクションが連鎖する
		log.Printf("[InventoryService] 在庫不足: productID=%s, 現在庫=%d, 要求数=%d",
			payload.ProductID, stock, payload.Quantity)
		s.bus.Publish(Event{
			Type: EventInventoryReserveFailed,
			Payload: InventoryReserveFailedPayload{
				OrderID: payload.OrderID,
				Reason:  "在庫不足",
			},
		})
		return
	}

	// 在庫確保成功
	s.inventory[payload.ProductID] -= payload.Quantity
	s.reserved[payload.OrderID] = reservedItem{
		ProductID: payload.ProductID,
		Quantity:  payload.Quantity,
	}
	log.Printf("[InventoryService] 在庫確保成功: orderID=%s, productID=%s, 残在庫=%d",
		payload.OrderID, payload.ProductID, s.inventory[payload.ProductID])

	// InventoryReservedイベントを発行
	s.bus.Publish(Event{
		Type: EventInventoryReserved,
		Payload: InventoryReservedPayload{
			OrderID: payload.OrderID,
		},
	})

	// Sagaの最終ステップ: OrderCompletedイベントを発行して注文を完了させる
	s.bus.Publish(Event{
		Type: EventOrderCompleted,
		Payload: OrderCompletedPayload{
			OrderID: payload.OrderID,
		},
	})
}

// releaseInventory は補償トランザクションとして確保した在庫を解放する
// OrderCancelledイベントを受信したときに呼ばれる
func (s *InventoryService) releaseInventory(orderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.reserved[orderID]
	if !exists {
		// この注文の在庫確保が行われていない場合はスキップ（冪等性）
		return
	}

	log.Printf("[InventoryService] 補償トランザクション: 在庫解放: orderID=%s, productID=%s, quantity=%d",
		orderID, item.ProductID, item.Quantity)

	// 在庫を元に戻す
	s.inventory[item.ProductID] += item.Quantity
	delete(s.reserved, orderID)

	s.bus.Publish(Event{
		Type: EventInventoryReleased,
		Payload: InventoryReleasedPayload{
			OrderID: orderID,
		},
	})
}
