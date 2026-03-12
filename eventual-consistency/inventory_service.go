package main

import (
	"log"
	"sync"
)

// InventoryService は在庫サービス
// メモリ上の在庫マップを管理し、注文イベントに応じて在庫を操作する
type InventoryService struct {
	mu        sync.Mutex
	inventory map[string]int // productID -> 在庫数

	// 注文サービスへのイベント返却チャンネル（参照を保持）
	orderConfirmedCh chan<- OrderConfirmedEvent
	orderFailedCh    chan<- OrderFailedEvent
}

// NewInventoryService は在庫サービスを初期化して返す
// 初期在庫データをセットアップする
func NewInventoryService(
	confirmedCh chan<- OrderConfirmedEvent,
	failedCh chan<- OrderFailedEvent,
) *InventoryService {
	return &InventoryService{
		inventory: map[string]int{
			"product-A": 10,
			"product-B": 2,
			"product-C": 0, // 在庫なし（失敗シナリオ用）
		},
		orderConfirmedCh: confirmedCh,
		orderFailedCh:    failedCh,
	}
}

// ListenForOrders はOrderCreatedEventを受信して在庫操作を行う
// contextでgoroutineのライフサイクルを管理する
func (s *InventoryService) ListenForOrders(
	ctx interface{ Done() <-chan struct{} },
	orderCreatedCh <-chan OrderCreatedEvent,
) {
	log.Println("[InventoryService] 注文イベントリスナーを開始します")
	for {
		select {
		case event := <-orderCreatedCh:
			// 注文作成イベントを受信して在庫確認・確保を試みる
			s.processOrderCreated(event)
		case <-ctx.Done():
			log.Println("[InventoryService] 注文イベントリスナーを停止します")
			return
		}
	}
}

// processOrderCreated は注文に対して在庫確保を試みる
// 成功時はOrderConfirmedEvent、失敗時はOrderFailedEventを発行する
func (s *InventoryService) processOrderCreated(event OrderCreatedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stock, exists := s.inventory[event.ProductID]
	if !exists {
		// 商品自体が存在しない
		log.Printf("[InventoryService] 商品が存在しません: productID=%s, orderID=%s",
			event.ProductID, event.OrderID)
		s.orderFailedCh <- OrderFailedEvent{
			OrderID: event.OrderID,
			Reason:  "商品が存在しません",
		}
		return
	}

	if stock < event.Quantity {
		// 在庫不足: OrderFailedEventを発行（結果整合性: 非同期で注文サービスに通知）
		log.Printf("[InventoryService] 在庫不足: productID=%s, 現在庫=%d, 要求数=%d, orderID=%s",
			event.ProductID, stock, event.Quantity, event.OrderID)
		s.orderFailedCh <- OrderFailedEvent{
			OrderID: event.OrderID,
			Reason:  "在庫不足",
		}
		return
	}

	// 在庫を減らす
	s.inventory[event.ProductID] -= event.Quantity
	log.Printf("[InventoryService] 在庫を確保しました: productID=%s, 残在庫=%d, orderID=%s",
		event.ProductID, s.inventory[event.ProductID], event.OrderID)

	// OrderConfirmedEventを発行（注文サービスに成功を通知）
	s.orderConfirmedCh <- OrderConfirmedEvent{
		OrderID: event.OrderID,
	}
}

// GetStock は商品の現在庫数を返す
func (s *InventoryService) GetStock(productID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inventory[productID]
}
