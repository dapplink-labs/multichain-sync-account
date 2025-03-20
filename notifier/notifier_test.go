package notifier

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapplink-labs/multichain-sync-account/database"
)

func setupNotifier(t *testing.T) *Notifier {
	db := database.SetupDb()

	ctx := context.Background()
	_, cancelCauseFunc := context.WithCancelCause(ctx)

	newNotifier, _ := NewNotifier(db, cancelCauseFunc)

	return newNotifier
}

func Test_Notifier_start(t *testing.T) {
	notifier := setupNotifier(t)

	err := notifier.Start(context.Background())
	assert.NoError(t, err)

	// 等待一段时间让 worker 处理交易
	time.Sleep(1000 * time.Second)
}
