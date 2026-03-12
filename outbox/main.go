package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// main はOutboxパターンのデモを実行する
//
// 【Outboxパターンとは】
// DBへのデータ書き込みとメッセージ発行を同一トランザクションで行うことで、
// 「データは保存されたがメッセージは送信されなかった」という不整合を防ぐパターン。
//
// 流れ:
// 1. OrderServiceが注文データとOutboxメッセージをDBに同時保存（トランザクション）
// 2. OutboxRelayが定期的にOutboxテーブルをポーリング
// 3. 未送信メッセージをMessageBrokerに送信
// 4. 送信成功後にProcessedAtを更新（冪等性保証）
//
// これにより、サービス障害が発生してもメッセージは必ず配信される（At-least-once配信）
func main() {
	log.Println("=== Outboxパターン デモ開始 ===")

	// contextでgoroutineのライフサイクルを管理
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// コンポーネントの初期化
	db := NewInMemoryDB()
	broker := NewMessageBroker(20)
	orderSvc := NewOrderService(db)
	relay := NewOutboxRelay(db, broker)

	// メッセージブローカーのコンシューマーをgoroutineで起動
	go consumeMessages(ctx, broker)

	// OutboxRelayをgoroutineで起動（500ms間隔でポーリング）
	go relay.Start(ctx, 500*time.Millisecond)

	// OutboxRelayが起動するまで少し待機
	time.Sleep(100 * time.Millisecond)

	// シナリオ1: 通常の注文作成
	log.Println("\n--- シナリオ1: 注文を作成します ---")
	order1, err := orderSvc.SaveOrderWithOutbox("user-1", "product-A", 2, 1000)
	if err != nil {
		log.Fatalf("[Main] 注文作成エラー: %v", err)
	}
	log.Printf("[Main] 注文1を作成しました: orderID=%s", order1.ID)

	// シナリオ2: 別の注文を即座に作成（OutboxRelayは非同期で処理する）
	log.Println("\n--- シナリオ2: 続けて別の注文を作成します ---")
	order2, err := orderSvc.SaveOrderWithOutbox("user-2", "product-B", 1, 2000)
	if err != nil {
		log.Fatalf("[Main] 注文作成エラー: %v", err)
	}
	log.Printf("[Main] 注文2を作成しました: orderID=%s", order2.ID)

	// OutboxRelayがポーリングしてメッセージを処理するまで待機
	log.Println("\n[Main] OutboxRelayがメッセージを処理するまで待機します...")
	time.Sleep(1500 * time.Millisecond)

	// シナリオ3: さらに注文を追加（RelayがすでにRunning状態であることを確認）
	log.Println("\n--- シナリオ3: OutboxRelayが稼働中に追加の注文を作成します ---")
	order3, err := orderSvc.SaveOrderWithOutbox("user-3", "product-C", 5, 3000)
	if err != nil {
		log.Fatalf("[Main] 注文作成エラー: %v", err)
	}
	log.Printf("[Main] 注文3を作成しました: orderID=%s", order3.ID)

	// 最終的なOutboxの状態を確認
	time.Sleep(1500 * time.Millisecond)

	log.Println("\n--- Outboxテーブルの最終状態 ---")
	printOutboxStatus(db)

	// cancelでgoroutineを停止
	cancel()
	time.Sleep(100 * time.Millisecond)

	log.Println("\n=== Outboxパターン デモ終了 ===")
}

// consumeMessages はMessageBrokerからメッセージを受信して処理するコンシューマー
// 実際のシステムではダウンストリームサービス（在庫サービスなど）が処理する
func consumeMessages(ctx context.Context, broker *MessageBroker) {
	log.Println("[Consumer] メッセージコンシューマーを開始します")
	for {
		select {
		case msg := <-broker.Receive():
			log.Printf("[Consumer] メッセージを受信しました: outboxID=%s, eventType=%s, aggregateID=%s, payload=%s",
				msg.ID, msg.EventType, msg.AggregateID, msg.Payload)
		case <-ctx.Done():
			log.Println("[Consumer] メッセージコンシューマーを停止します")
			return
		}
	}
}

// printOutboxStatus はOutboxテーブルの処理状態をログ出力する
func printOutboxStatus(db *InMemoryDB) {
	db.outboxMu.RLock()
	defer db.outboxMu.RUnlock()

	for _, msg := range db.outbox {
		status := "未処理"
		if msg.ProcessedAt != nil {
			status = fmt.Sprintf("処理済み (%s)", msg.ProcessedAt.Format("15:04:05.000"))
		}
		log.Printf("[Main] Outbox: id=%s, eventType=%s, aggregateID=%s, status=%s",
			msg.ID, msg.EventType, msg.AggregateID, status)
	}
}
