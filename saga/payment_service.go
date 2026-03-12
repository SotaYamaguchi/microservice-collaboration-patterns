package main

import (
	"log"
)

// PaymentProcessedPayload は支払い完了イベントのペイロード
type PaymentProcessedPayload struct {
	OrderID   string
	UserID    string
	ProductID string
	Quantity  int
	Amount    int
}

// PaymentFailedPayload は支払い失敗イベントのペイロード
type PaymentFailedPayload struct {
	OrderID string
	Reason  string
}

// PaymentRefundedPayload は支払い返金イベントのペイロード（補償トランザクション）
type PaymentRefundedPayload struct {
	OrderID string
	Amount  int
}

// PaymentService は支払いサービス
// OrderCreatedイベントを受信して支払い処理を行い、結果をイベントとして発行する
type PaymentService struct {
	bus *EventBus
}

// NewPaymentService は支払いサービスを初期化して返す
func NewPaymentService(bus *EventBus) *PaymentService {
	return &PaymentService{bus: bus}
}

// Start はイベントリスナーを起動する（goroutineで実行）
func (s *PaymentService) Start(ctx interface{ Done() <-chan struct{} }) {
	// 注文作成イベントを購読（Sagaの第1ステップ）
	orderCreatedCh := s.bus.Subscribe(EventOrderCreated)
	// 注文キャンセルイベントを購読（補償トランザクションのトリガー）
	orderCancelledCh := s.bus.Subscribe(EventOrderCancelled)

	log.Println("[PaymentService] イベントリスナーを開始します")
	for {
		select {
		case event := <-orderCreatedCh:
			// 注文作成イベントを受信: 支払い処理を実行
			payload := event.Payload.(OrderCreatedPayload)
			s.processPayment(payload)

		case event := <-orderCancelledCh:
			// 注文キャンセルイベントを受信: 補償トランザクション（返金）を実行
			payload := event.Payload.(OrderCancelledPayload)
			s.refundPayment(payload.OrderID)

		case <-ctx.Done():
			log.Println("[PaymentService] イベントリスナーを停止します")
			return
		}
	}
}

// processPayment は支払い処理を行い、結果をイベントとして発行する
// シミュレーション: 金額が10000以上の場合は失敗とする
func (s *PaymentService) processPayment(payload OrderCreatedPayload) {
	log.Printf("[PaymentService] 支払い処理を開始します: orderID=%s, amount=%d",
		payload.OrderID, payload.Amount)

	if payload.Amount >= 10000 {
		// 支払い失敗: 残高不足などのシミュレーション
		log.Printf("[PaymentService] 支払い失敗: orderID=%s, amount=%d (残高不足)", payload.OrderID, payload.Amount)
		s.bus.Publish(Event{
			Type: EventPaymentFailed,
			Payload: PaymentFailedPayload{
				OrderID: payload.OrderID,
				Reason:  "残高不足",
			},
		})
		return
	}

	// 支払い成功: PaymentProcessedイベントを発行してSagaの次ステップへ
	log.Printf("[PaymentService] 支払い完了: orderID=%s, amount=%d", payload.OrderID, payload.Amount)
	s.bus.Publish(Event{
		Type: EventPaymentProcessed,
		Payload: PaymentProcessedPayload{
			OrderID:   payload.OrderID,
			UserID:    payload.UserID,
			ProductID: payload.ProductID,
			Quantity:  payload.Quantity,
			Amount:    payload.Amount,
		},
	})
}

// refundPayment は補償トランザクションとして返金処理を行う
// 在庫確保失敗時に呼ばれ、すでに処理した支払いをキャンセルする
func (s *PaymentService) refundPayment(orderID string) {
	log.Printf("[PaymentService] 補償トランザクション: 返金処理を実行します: orderID=%s", orderID)

	// 返金処理（ここではシミュレーション）
	s.bus.Publish(Event{
		Type: EventPaymentRefunded,
		Payload: PaymentRefundedPayload{
			OrderID: orderID,
		},
	})
	log.Printf("[PaymentService] 返金完了: orderID=%s", orderID)
}
