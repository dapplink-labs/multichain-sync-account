package worker

import (
	"context"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient/chain-account/account"
)

const (
	depositBusinessId = "1"

	CurrentRequestId = 1
	CurrentChainId   = 17000
	CurrentChain     = "ethereum"
)

var CurrentBlockNumber = new(big.Int).SetUint64(200000)

func setupDeposit(t *testing.T) *Deposit {
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
			ChainId:   CurrentChainId,
			ChainName: CurrentChain,

			StartingHeight: 2849348,
			Confirmations:  64,
			BlocksStep:     1,

			WorkerInterval:       10 * time.Second,
			SynchronizerInterval: 10 * time.Second,
		},
	}

	// 创建 shutdown 函数
	shutdown := func(cause error) {
		t.Logf("Shutdown called with cause: %v", cause)
	}

	// 创建 Deposit worker
	deposit, err := NewDeposit(cfg, db, accountClient, shutdown)
	assert.NoError(t, err)

	return deposit
}

func TestDeposit_BaseSynchronizer_Start(t *testing.T) {
	// 准备测试环境
	deposit := setupDeposit(t)

	// 启动 worker
	err := deposit.BaseSynchronizer.Start()
	assert.NoError(t, err)

	// 等待一段时间让 worker 处理交易
	time.Sleep(1000 * time.Second)

	// 清理资源
	err = deposit.Close()
	assert.NoError(t, err)
}

func TestDeposit_Start(t *testing.T) {
	// 准备测试环境
	deposit := setupDeposit(t)

	// 启动 worker
	err := deposit.Start()
	assert.NoError(t, err)

	// 等待一段时间让 worker 处理交易
	time.Sleep(1000 * time.Second)

	// 清理资源
	err = deposit.Close()
	assert.NoError(t, err)
}

func TestDeposit_SendTransaction(t *testing.T) {
	// 准备测试环境
	deposit := setupDeposit(t)

	depositTxId := "818e6568-17ee-463b-ad29-ea05adcc664d"

	dbDeposit, err := deposit.database.Deposits.QueryDepositsById(strconv.Itoa(CurrentRequestId), depositTxId)
	assert.NoError(t, err)

	// 模拟发送交易上链
	sendTx, err := deposit.rpcClient.SendTx(dbDeposit.TxSignHex)
	assert.NoError(t, err)

	dbDeposit.TxHash = common.HexToHash(sendTx)
	dbDeposit.Status = database.TxStatusBroadcasted

	err = deposit.database.Deposits.UpdateDepositListById(strconv.Itoa(CurrentRequestId), []*database.Deposits{dbDeposit})
	assert.NoError(t, err)
}
func TestDeposit_depostit(t *testing.T) {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stdout, log.LevelInfo, true)))
	// 准备测试环境
	deposit := setupDeposit(t)
	var balances []*database.TokenBalance
	balancesamount, _ := new(big.Int).SetString("30000000000000000", 10)
	balances = append(
		balances,
		&database.TokenBalance{
			FromAddress:  common.HexToAddress("0x101b37f4544c26047d37df59b13a2444eda192a1"),
			ToAddress:    common.HexToAddress("0x90c35397f35b54f20060c958074ebe9646a1957e"),
			TokenAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			Balance:      balancesamount,
			TxType:       database.TxTypeDeposit,
		},
	)

	err := deposit.database.Balances.UpdateOrCreate("xiaohuolong", balances)

	assert.NoError(t, err)

}

func TestHandleBatch(t *testing.T) {
	deposit := setupDeposit(t)
	businessIdV2 := strconv.Itoa(CurrentRequestId)

	//depositTx := &Transaction{
	//	BusinessId:     businessIdV2,
	//	BlockNumber:    big.NewInt(2894897),
	//	FromAddress:    "0xDBbd037428E2ae9D540F09253b2EcCc6F60079a8",
	//	ToAddress:      "0xD79053a14BC465d9C1434d4A4fAbdeA7b6a2A94b",
	//	Hash:           "0x479360e883ec03d84e5cb6a13194463c8c674469f82df9d17d6a87385ef668fc",
	//	TokenAddress:   "0x00",
	//	ContractWallet: "0xContractWallet",
	//	TxType:         "deposit",
	//}

	withdrawTx := &Transaction{
		BusinessId:     businessIdV2,
		BlockNumber:    big.NewInt(2880690),
		FromAddress:    "0xDf894d39f6b33763bf55582Bb7A8b5515bccD982",
		ToAddress:      "0xDBbd037428E2ae9D540F09253b2EcCc6F60079a8",
		Hash:           "0x21f43c1eb3970e4d9c1ded367b440131af56dc09fedaadb8a2d8475a53d52741",
		TokenAddress:   "0x00",
		ContractWallet: "0xContractWallet",
		TxType:         "withdraw",
	}

	//internalTx := &Transaction{
	//	BusinessId:     businessIdV2,
	//	BlockNumber:    big.NewInt(2880492),
	//	FromAddress:    "0xD79053a14BC465d9C1434d4A4fAbdeA7b6a2A94b",
	//	ToAddress:      "0xDf894d39f6b33763bf55582Bb7A8b5515bccD982",
	//	Hash:           "0x967f6cf1a29562cfafb9a8cbd7cd3aa3e191a92922eaf1c2588fa418feab0c01",
	//	TokenAddress:   "0x00",
	//	ContractWallet: "0xContractWallet",
	//	TxType:         "collection",
	//}

	//invalidTx := &Transaction{
	//	BusinessId:     businessIdV2,
	//	BlockNumber:    big.NewInt(2895229),
	//	FromAddress:    "0xSenderAddress",
	//	ToAddress:      "0xReceiverAddress",
	//	Hash:           "0x4567890abcdef123",
	//	TokenAddress:   "0x00",
	//	ContractWallet: "0xContractWallet",
	//	TxType:         "unknow",
	//}

	batch := map[string]*TransactionsChannel{
		businessIdV2: {
			BlockHeight: 2895229,
			//Transactions: []*Transaction{depositTx, withdrawTx, internalTx, invalidTx},
			//Transactions: []*Transaction{depositTx, withdrawTx, internalTx},
			Transactions: []*Transaction{withdrawTx},
		},
	}

	err := deposit.handleBatch(batch)
	assert.NoError(t, err)

	//dbDeposit, err := deposit.database.Deposits.QueryDepositsById(businessIdV2, depositTx.Hash)
	//assert.NoError(t, err)
	//assert.Equal(t, database.TxStatusCreateUnsigned, dbDeposit.Status)
	//assert.Equal(t, common.HexToHash(depositTx.Hash), dbDeposit.TxHash)
}

func TestProcessBatch(t *testing.T) {
	deposit := setupDeposit(t)
	businessIdV2 := strconv.Itoa(CurrentRequestId)

	depositBlockHeader, err := deposit.BaseSynchronizer.rpcClient.GetBlockHeader(big.NewInt(2894897))
	assert.NoError(t, err)
	withdrawBlockHeader, err := deposit.BaseSynchronizer.rpcClient.GetBlockHeader(big.NewInt(2880690))
	assert.NoError(t, err)
	internalBlockHeader, err := deposit.BaseSynchronizer.rpcClient.GetBlockHeader(big.NewInt(2880492))
	assert.NoError(t, err)
	invalidBlockHeader, err := deposit.BaseSynchronizer.rpcClient.GetBlockHeader(big.NewInt(2895229))
	assert.NoError(t, err)

	blockHeaderList := []rpcclient.BlockHeader{*depositBlockHeader, *withdrawBlockHeader, *internalBlockHeader, *invalidBlockHeader}

	err = deposit.BaseSynchronizer.processBatch(blockHeaderList)
	assert.NoError(t, err)

	select {
	case businessTxChannel := <-deposit.BaseSynchronizer.businessChannels:
		assert.NotNil(t, businessTxChannel[businessIdV2])
		t.Logf("Response:\n%s", json2.ToPrettyJSON(businessTxChannel))
	default:
		t.Fatal("Expected businessTxChannel to have data")
	}

	time.Sleep(1000 * time.Second)
}
