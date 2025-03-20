package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/account"
)

const (
	internalBusinessId = "xiaohuolong"
	internalTxId       = "1e7e508b-5ad0-4ba7-b92c-b8bc1555fd9b"
)

func setupInternal(t *testing.T) *Internal {
	// 设置数据库
	db := database.SetupDb()

	// 设置 gRPC 连接
	conn, err := grpc.NewClient("127.0.0.1:8189", grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	// 设置账户客户端
	client := account.NewWalletAccountServiceClient(conn)
	accountClient, err := rpcclient.NewWalletChainAccountClient(context.Background(), client, "Ethereum")
	assert.NoError(t, err)

	// 设置配置
	cfg := &config.Config{
		ChainNode: config.ChainNodeConfig{
			WorkerInterval: 5 * time.Second,
		},
	}

	// 创建 shutdown 函数
	shutdown := func(cause error) {
		t.Logf("Shutdown called with cause: %v", cause)
	}

	// 创建 Withdraw worker
	internal, err := NewInternal(cfg, db, accountClient, shutdown)
	assert.NoError(t, err)

	return internal
}

func TestInternal_Start(t *testing.T) {
	// 准备测试环境
	internal := setupInternal(t)

	// 启动 worker
	err := internal.Start()
	assert.NoError(t, err)

	// 等待一段时间让 worker 处理交易
	time.Sleep(1000 * time.Second)

	// 清理资源
	err = internal.Close()
	assert.NoError(t, err)
}
