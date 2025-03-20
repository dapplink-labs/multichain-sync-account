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
	notifyUrl        = "http://127.0.0.1:8001"
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

func TestBusinessMiddleWireServices_ExportAddressesByPublicKeys(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 测试成功导出地址
	t.Run("successful export addresses", func(t *testing.T) {
		// 构造请求
		request := &dal_wallet_go.ExportAddressesRequest{
			ConsumerToken: "test_token",
			RequestId:     CurrentRequestId,
			PublicKeys: []*dal_wallet_go.PublicKey{
				{
					Type:      "eoa",
					PublicKey: "0422d39a1208b314bbbae7545c0b415167386d448ba9777b526e56d458db2f9f70d72f89373b7f53dfc9f0ff6aa55ae736fe2160d7ddd8be470250dd23fae9b0bc",
				},
				{
					Type:      "hot",
					PublicKey: "047b40b2707107640641c983919bfff36946849df442564a9bccc577680898c7449546e54eb4a2f63bfe8f061c9d7b7f6669a3154479746cc8e0d7c6ca2d490e6a",
				},
				{
					Type:      "cold",
					PublicKey: "04a84731792f6cdfb67d1c591d090844af1ecf4bb73193c7e389fedbdfc088564b3a1f9372781a0d92feb4251b3059f050873ada6ac2cb9b5b40f709900ce2a65d",
				},
			},
		}

		// 执行测试
		response, err := bws.ExportAddressesByPublicKeys(ctx, request)

		// 打印响应详情
		respJSON := json2.ToPrettyJSON(response)
		t.Logf("Response:\n%s", respJSON)

		// 验证结果
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, dal_wallet_go.ReturnCode_SUCCESS, response.Code)
		assert.NotEmpty(t, response.Addresses)
		assert.Equal(t, len(request.PublicKeys), len(response.Addresses))

		// 验证每个地址的格式和类型
		for _, addr := range response.Addresses {
			assert.True(t, strings.HasPrefix(addr.Address, "0x"))
			assert.Equal(t, 42, len(addr.Address)) // 以太坊地址长度为42（包含"0x"前缀）
		}
	})
}

func TestBusinessMiddleWireServices_CreateUnSignTransaction_collection(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 构造请求
	request := &dal_wallet_go.UnSignTransactionRequest{
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

func TestBusinessMiddleWireServices_CreateUnSignTransaction_hot2cold(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 构造请求
	request := &dal_wallet_go.UnSignTransactionRequest{
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
		TxType:          "hot2cold",
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

func TestBusinessMiddleWireServices_CreateUnSignTransaction_withdraw(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 构造请求
	request := &dal_wallet_go.UnSignTransactionRequest{
		ConsumerToken: "test_token",
		RequestId:     CurrentRequestId,
		ChainId:       CurrentChainId, // 主网
		Chain:         CurrentChain,
		From:          "0xDf894d39f6b33763bf55582Bb7A8b5515bccD982",
		To:            "0xDBbd037428E2ae9D540F09253b2EcCc6F60079a8",
		//Value:         "1000000000000000000", // 1 ETH
		//Value:           "10000000000000000", // 0.01 ETH
		Value:           "1000000000000000", // 0.001 ETH
		ContractAddress: "0x00",
		TokenId:         "",
		TokenMeta:       "",
		TxType:          "withdraw",
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

func TestBusinessMiddleWireServices_CreateUnSignTransaction_deposit(t *testing.T) {
	// 准备测试环境
	bws := setup(t)
	ctx := context.Background()

	// 构造请求
	request := &dal_wallet_go.UnSignTransactionRequest{
		ConsumerToken: "test_token",
		RequestId:     CurrentRequestId,
		ChainId:       CurrentChainId, // 主网
		Chain:         CurrentChain,
		From:          "0xDBbd037428E2ae9D540F09253b2EcCc6F60079a8",
		To:            "0xD79053a14BC465d9C1434d4A4fAbdeA7b6a2A94b",
		//Value:         "1000000000000000000", // 1 ETH
		//Value:           "10000000000000000", // 0.01 ETH
		Value:           "1000000000000000", // 0.001 ETH
		ContractAddress: "0x00",
		TokenId:         "",
		TokenMeta:       "",
		TxType:          "deposit",
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
	request := &dal_wallet_go.SignedTransactionRequest{
		ConsumerToken: "test_token",
		RequestId:     CurrentRequestId,
		Chain:         CurrentChain,
		ChainId:       CurrentChainId,
		TransactionId: "818e6568-17ee-463b-ad29-ea05adcc664d",
		Signature:     "237e717ed1c5e8234d1f7407953db778b2f273f779b77d3eb72d100b7331de4471d7b79ab5a6a97543d4fdd7e176d6259dab7fd2a493cede8938a9a8621c32cc00",
		TxType:        "deposit",
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
