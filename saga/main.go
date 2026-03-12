package main

import (
	"context"
	"log"
	"time"
)

// main はSagaパターン（コレオグラフィー）のデモを実行する
//
// 【Sagaパターン（コレオグラフィー）とは】
// 各サービスがイベントを発行・購読し合うことで分散トランザクションを実現するパターン。
// 中央のオーケストレーターは存在せず、各サービスが自律的に動作する。
// 処理失敗時は補償トランザクション（逆操作）を実行してデータを整合させる。
//
// 正常フロー:
//   OrderCreated → PaymentProcessed → InventoryReserved → OrderCompleted
//
// 失敗フロー（支払い失敗）:
//   OrderCreated → PaymentFailed → OrderCancelled
//
// 失敗フロー（在庫不足）:
//   OrderCreated → PaymentProcessed → InventoryReserveFailed → OrderCancelled → PaymentRefunded
func main() {
	log.Println("=== Sagaパターン（コレオグラフィー）デモ開始 ===")

	// contextでgoroutineのライフサイクルを管理
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// イベントバス（シンプルなpub/sub）を初期化
	bus := NewEventBus()

	// 各サービスを初期化
	orderSvc := NewOrderService(bus)
	paymentSvc := NewPaymentService(bus)
	inventorySvc := NewInventoryService(bus)

	// 各サービスのイベントリスナーをgoroutineで起動
	go orderSvc.Start(ctx)
	go paymentSvc.Start(ctx)
	go inventorySvc.Start(ctx)

	// goroutineが起動するまで少し待機
	time.Sleep(100 * time.Millisecond)

	// シナリオ1: 正常完了（支払い成功 + 在庫あり）
	log.Println("\n--- シナリオ1: 正常完了（支払い成功 + 在庫あり） ---")
	order1ID := orderSvc.CreateOrder("user-1", "product-A", 2, 1000)
	time.Sleep(500 * time.Millisecond)
	printOrderStatus(orderSvc, order1ID)

	// シナリオ2: 支払い失敗
	log.Println("\n--- シナリオ2: 支払い失敗（残高不足） ---")
	order2ID := orderSvc.CreateOrder("user-poor", "product-A", 1, 999999) // 高額で失敗
	time.Sleep(500 * time.Millisecond)
	printOrderStatus(orderSvc, order2ID)

	// シナリオ3: 在庫不足（支払い後に在庫確保失敗 → 補償トランザクション）
	log.Println("\n--- シナリオ3: 在庫不足（支払い後に在庫確保失敗 → PaymentRefunded補償） ---")
	order3ID := orderSvc.CreateOrder("user-3", "product-B", 100, 500) // product-Bの在庫は5
	time.Sleep(500 * time.Millisecond)
	printOrderStatus(orderSvc, order3ID)

	// cancelでgoroutineを停止
	cancel()
	time.Sleep(100 * time.Millisecond)

	log.Println("\n=== Sagaパターン デモ終了 ===")
}

// printOrderStatus は注文の最終状態をログ出力する
func printOrderStatus(svc *OrderService, orderID string) {
	if order, ok := svc.GetOrder(orderID); ok {
		log.Printf("[Main] 最終状態: orderID=%s, status=%s", order.ID, order.Status)
	}
}
