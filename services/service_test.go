package services

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	dal_wallet_go "github.com/dapplink-labs/multichain-sync-account/protobuf/dal-wallet-go"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/account"
)

const (
	notifyUrl        = "127.0.0.1:8001/dapplink/notify"
	CurrentRequestId = "1"
	CurrentChainId   = "17000"
	CurrentChain     = "ethereum"
)

func setupDb() *database.DB {
	dbConfig := config.DBConfig{
		Host:     "127.0.0.1",
		Port:     5432,
		Name:     "multichain",
		User:     "postgres",
		Password: "123456",
	}

	newDB, _ := database.NewDB(context.Background(), dbConfig)
	return newDB
}

func setup(t *testing.T) *BusinessMiddleWireServices {
	db := setupDb()

	bConfig := &BusinessMiddleConfig{
		GrpcHostname: "localhost",
		GrpcPort:     50051,
	}

	conn, err := grpc.NewClient("127.0.0.1:8189", grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	client := account.NewWalletAccountServiceClient(conn)
	accountClient, err := rpcclient.NewWalletChainAccountClient(context.Background(), client, "Ethereum")

	bws, err := NewBusinessMiddleWireServices(db, bConfig, accountClient)
	assert.NoError(t, err)

	return bws
}

func TestBusinessMiddleWireServices_BusinessRegister(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 测试成功注册
	t.Run("successful registration", func(t *testing.T) {
		request := &dal_wallet_go.BusinessRegisterRequest{
			ConsumerToken: "test_token",
			RequestId:     CurrentRequestId,
			NotifyUrl:     notifyUrl,
		}

		businessRegisterResponse, err := bws.BusinessRegister(ctx, request)
		// 验证结果
		assert.NoError(t, err)
		assert.NotNil(t, businessRegisterResponse)
		assert.Equal(t, dal_wallet_go.ReturnCode_SUCCESS, businessRegisterResponse.Code)
		// 打印响应详情
		respJSON := json2.ToPrettyJSON(businessRegisterResponse)
		t.Logf("BusinessRegister:\n%s", respJSON)

		// 验证数据库存储
		business, err := bws.db.Business.QueryBusinessByUuid(request.RequestId)
		assert.NoError(t, err)
		assert.NotNil(t, business)
		businessJSON := json2.ToPrettyJSON(business)
		t.Logf("QueryBusinessByUuid:\n%s", businessJSON)
	})
}

func TestBusinessMiddleWireServices_CreateUnSignTransaction_ETHTransfer(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 构造请求
	request := &dal_wallet_go.UnSignWithdrawTransactionRequest{
		ConsumerToken: "test_token",
		RequestId:     CurrentRequestId,
		ChainId:       CurrentChainId, // 主网
		Chain:         CurrentChain,
		From:          "0xD79053a14BC465d9C1434d4A4fAbdeA7b6a2A94b",
		To:            "0xDf894d39f6b33763bf55582Bb7A8b5515bccD982",
		//Value:         "1000000000000000000", // 1 ETH
		Value:           "10000000000000000", // 0.01 ETH
		ContractAddress: "0x00",
		TokenId:         "",
		TokenMeta:       "",
		TxType:          "collection",
	}

	// 执行测试
	response, err := bws.BuildUnSignTransaction(ctx, request)

	// 打印响应详情
	respJSON := json2.ToPrettyJSON(response)
	t.Logf("Response:\n%s", respJSON)

	// 验证结果
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, dal_wallet_go.ReturnCode_SUCCESS, response.Code)
	assert.NotEmpty(t, response.TransactionId)
	assert.NotEmpty(t, response.UnSignTx)

	// 可以添加更详细的验证
	// 例如验证 UnSignTx 的格式是否正确
	assert.True(t, strings.HasPrefix(response.UnSignTx, "0x"))

	//respJSON2, _ := json.Marshal(response)
	//t.Logf("Response:\n%s", string(respJSON2))
}

func TestBusinessMiddleWireServices_BuildSignedTransaction(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 2. 先创建一个未签名交易
	request := &dal_wallet_go.SignedWithdrawTransactionRequest{
		ConsumerToken: "test_token",
		RequestId:     CurrentRequestId,
		Chain:         CurrentChain,
		ChainId:       CurrentChainId,
		TransactionId: "5d6fc7d7-b452-4b4f-96f0-1ff358fd1beb",
		Signature:     "6a4a724e6986c88f0300b140409cb8405595c1317a5f744003ce92fbc36f06cd5737a3cf956a9fc551da23ae6cb102601c0e18190dfd2bdbbbf3d5b9115eb81a00",
		TxType:        "collection",
	}

	// 执行测试
	response, err := bws.BuildSignedTransaction(ctx, request)

	// 打印响应详情
	respJSON := json2.ToPrettyJSON(response)
	t.Logf("Response:\n%s", respJSON)

	// 验证结果
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, dal_wallet_go.ReturnCode_SUCCESS, response.Code)
}
