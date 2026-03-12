package main

import (
	"context"
	"log"
	"time"
)

// main は結果整合性パターンのデモを実行する
//
// 【結果整合性とは】
// 複数のサービスが独立して動作し、即時の整合性は保証しないが
// 最終的には整合した状態になることを保証するパターン。
// 注文作成直後は "pending" 状態で、在庫サービスが処理した後に
// "confirmed" または "failed" に遷移する。
func main() {
	log.Println("=== 結果整合性パターン デモ開始 ===")

	// contextでgoroutineのライフサイクルを管理
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 注文サービスを初期化
	orderSvc := NewOrderService()

	// 在庫サービスを初期化（注文サービスのチャンネルを渡す）
	inventorySvc := NewInventoryService(
		orderSvc.OrderConfirmedCh,
		orderSvc.OrderFailedCh,
	)

	// 注文サービスのステータス更新リスナーをgoroutineで起動
	go orderSvc.ListenForStatusUpdates(ctx)

	// 在庫サービスの注文イベントリスナーをgoroutineで起動
	go inventorySvc.ListenForOrders(ctx, orderSvc.OrderCreatedCh)

	// goroutineが起動するまで少し待機
	time.Sleep(100 * time.Millisecond)

	log.Println("\n--- シナリオ1: 在庫あり（成功パターン） ---")
	order1 := orderSvc.CreateOrder("user-1", "product-A", 3)
	log.Printf("[Main] 注文作成直後のステータス: orderID=%s, status=%s (まだpending)", order1.ID, order1.Status)

	log.Println("\n--- シナリオ2: 在庫不足（失敗パターン） ---")
	order2 := orderSvc.CreateOrder("user-2", "product-B", 5) // product-Bの在庫は2
	log.Printf("[Main] 注文作成直後のステータス: orderID=%s, status=%s (まだpending)", order2.ID, order2.Status)

	log.Println("\n--- シナリオ3: 在庫なし商品（失敗パターン） ---")
	order3 := orderSvc.CreateOrder("user-3", "product-C", 1) // product-Cの在庫は0
	log.Printf("[Main] 注文作成直後のステータス: orderID=%s, status=%s (まだpending)", order3.ID, order3.Status)

	// 非同期処理が完了するまで待機（結果整合性: 時間が経てば整合する）
	time.Sleep(500 * time.Millisecond)

	log.Println("\n--- 最終状態の確認（結果整合性が達成された状態） ---")
	for _, orderID := range []string{order1.ID, order2.ID, order3.ID} {
		if order, ok := orderSvc.GetOrder(orderID); ok {
			log.Printf("[Main] 最終状態: orderID=%s, productID=%s, quantity=%d, status=%s",
				order.ID, order.ProductID, order.Quantity, order.Status)
		}
	}

	log.Printf("\n--- 在庫の最終状態 ---")
	for _, productID := range []string{"product-A", "product-B", "product-C"} {
		log.Printf("[Main] 在庫: productID=%s, stock=%d", productID, inventorySvc.GetStock(productID))
	}

	// cancelでgoroutineを停止
	cancel()
	time.Sleep(100 * time.Millisecond)

	log.Println("\n=== 結果整合性パターン デモ終了 ===")
}
