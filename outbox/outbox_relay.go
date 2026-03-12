package main

import (
	"context"
	"log"
	"time"
)

// MessageBroker はメッセージブローカーをシミュレートするチャンネル
// 実際のシステムではKafka、RabbitMQなどを使用する
type MessageBroker struct {
	ch chan OutboxMessage
}

// NewMessageBroker は新しいMessageBrokerを作成する
func NewMessageBroker(bufferSize int) *MessageBroker {
	return &MessageBroker{
		ch: make(chan OutboxMessage, bufferSize),
	}
}

// Send はメッセージをブローカーに送信する
func (b *MessageBroker) Send(msg OutboxMessage) error {
	// 実際のシステムではネットワーク越しにメッセージを送信する
	// ここではチャンネルに送信してシミュレート
	select {
	case b.ch <- msg:
		return nil
	default:
		// バッファが満杯の場合（実際はリトライやエラーハンドリングが必要）
		return nil
	}
}

// Receive はメッセージを受信するチャンネルを返す
func (b *MessageBroker) Receive() <-chan OutboxMessage {
	return b.ch
}

// OutboxRelay はOutboxテーブルをポーリングして未処理メッセージをブローカーに送信する
//
// 【OutboxRelayの役割】
// Outboxパターンでは、メッセージの送信はアプリケーションのトランザクションから切り離される。
// OutboxRelayが定期的にOutboxテーブルを確認し、未処理メッセージをブローカーに転送する。
// これにより、At-least-once配信（少なくとも1回は配信される）が保証される。
type OutboxRelay struct {
	db     *InMemoryDB
	broker *MessageBroker
}

// NewOutboxRelay は新しいOutboxRelayを作成する
func NewOutboxRelay(db *InMemoryDB, broker *MessageBroker) *OutboxRelay {
	return &OutboxRelay{
		db:     db,
		broker: broker,
	}
}

// Start はポーリングループをgoroutineで起動する
// intervalで指定した間隔でOutboxテーブルをポーリングする
func (r *OutboxRelay) Start(ctx context.Context, interval time.Duration) {
	log.Printf("[OutboxRelay] ポーリングを開始します: interval=%s", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期的にOutboxテーブルをポーリング
			r.processUnsentMessages()

		case <-ctx.Done():
			log.Println("[OutboxRelay] ポーリングを停止します")
			return
		}
	}
}

// processUnsentMessages は未送信のOutboxメッセージをすべて処理する
func (r *OutboxRelay) processUnsentMessages() {
	// 未処理メッセージを取得
	messages := r.db.GetUnprocessedMessages()
	if len(messages) == 0 {
		return
	}

	log.Printf("[OutboxRelay] 未処理メッセージを検出しました: count=%d", len(messages))

	for _, msg := range messages {
		if err := r.sendMessage(msg); err != nil {
			log.Printf("[OutboxRelay] メッセージ送信に失敗しました: outboxID=%s, error=%v", msg.ID, err)
			// 失敗した場合はスキップして次のポーリングで再試行（At-least-once配信）
			continue
		}

		// 送信成功時にProcessedAtを更新（冪等性: 二重送信を防ぐ）
		r.db.MarkAsProcessed(msg.ID)
	}
}

// sendMessage はメッセージをブローカーに送信する
func (r *OutboxRelay) sendMessage(msg *OutboxMessage) error {
	log.Printf("[OutboxRelay] メッセージを送信します: outboxID=%s, eventType=%s, aggregateID=%s",
		msg.ID, msg.EventType, msg.AggregateID)

	if err := r.broker.Send(*msg); err != nil {
		return err
	}

	log.Printf("[OutboxRelay] メッセージ送信完了: outboxID=%s", msg.ID)
	return nil
}
