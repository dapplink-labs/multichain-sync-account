package database

import (
	"math/big"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
)

func TestDepositsDB_StoreAndQuery(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	depositsDB := NewDepositsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	depositList := []*Deposits{
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567890,
			Status:               TxStatusCreateUnsigned,
			Confirms:             0,
			BlockHash:            common.HexToHash("0x1"),
			BlockNumber:          big.NewInt(1),
			TxHash:               common.HexToHash("0x2"),
			TxType:               TxTypeDeposit,
			FromAddress:          common.HexToAddress("0x3"),
			ToAddress:            common.HexToAddress("0x4"),
			Amount:               big.NewInt(1000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "10",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x5"),
			TokenId:              "1",
			TokenMeta:            "meta",
			TxSignHex:            "0x6",
		},
	}

	// Store the deposit
	err := depositsDB.StoreDeposits(requestId, depositList)
	if err != nil {
		t.Fatalf("failed to store deposit: %v", err)
	}

	// Query the deposit
	notifyDeposits, err := depositsDB.QueryNotifyDeposits(strconv.Itoa(CurrentRequestId))
	if err != nil {
		t.Fatalf("failed to query notify deposits: %v", err)
	}
	t.Logf("notifyDeposits %v", json2.ToPrettyJSON(notifyDeposits))

	blockNumber := uint64(10)
	confirms := uint64(5)
	err = depositsDB.UpdateDepositsComfirms(strconv.Itoa(CurrentRequestId), blockNumber, confirms)
	if err != nil {
		t.Fatalf("failed to update deposit confirms: %v", err)
	}

	// Query the deposit
	notifyDepositsV2, err := depositsDB.QueryNotifyDeposits(strconv.Itoa(CurrentRequestId))
	if err != nil {
		t.Fatalf("failed to query notify deposits: %v", err)
	}
	t.Logf("notifyDeposits_v2 %v", json2.ToPrettyJSON(notifyDepositsV2))
}

func TestUpdateDepositsNotifyStatus(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	depositsDB := NewDepositsDB(db.gorm)

	depositList := []*Deposits{
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567890,
			Status:               TxStatusCreateUnsigned,
			Confirms:             0,
			BlockHash:            common.HexToHash("0x1"),
			BlockNumber:          big.NewInt(1),
			TxHash:               common.HexToHash("0x2"),
			TxType:               TxTypeDeposit,
			FromAddress:          common.HexToAddress("0x3"),
			ToAddress:            common.HexToAddress("0x4"),
			Amount:               big.NewInt(1000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "10",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x5"),
			TokenId:              "1",
			TokenMeta:            "meta",
			TxSignHex:            "0x6",
		},
	}

	err := depositsDB.StoreDeposits(strconv.Itoa(CurrentRequestId), depositList)
	if err != nil {
		t.Fatalf("failed to store deposit: %v", err)
	}

	// Query the deposit
	notifyDeposits, err := depositsDB.QueryDepositsByTxHash(strconv.Itoa(CurrentRequestId), depositList[0].TxHash)
	if err != nil {
		t.Fatalf("failed to query notify deposits: %v", err)
	}
	t.Logf("notifyDeposits %v", json2.ToPrettyJSON(notifyDeposits))

	newStatus := TxStatusWalletDone
	err = depositsDB.UpdateDepositsStatusById(strconv.Itoa(CurrentRequestId), newStatus, depositList)
	if err != nil {
		t.Fatalf("failed to update deposit notify status: %v", err)
	}

	// Query the deposit
	notifyDepositsV2, err := depositsDB.QueryDepositsById(strconv.Itoa(CurrentRequestId), depositList[0].GUID.String())
	if err != nil {
		t.Fatalf("failed to query notify deposits: %v", err)
	}
	t.Logf("notifyDepositsV2 %v", json2.ToPrettyJSON(notifyDepositsV2))

}

func TestUpdateDepositList(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	depositsDB := NewDepositsDB(db.gorm)

	// Initial deposits to be stored
	depositList := []*Deposits{
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567890,
			Status:               TxStatusCreateUnsigned,
			Confirms:             0,
			BlockHash:            common.HexToHash("0x11"),
			BlockNumber:          big.NewInt(1),
			TxHash:               common.HexToHash("0x22"),
			TxType:               TxTypeDeposit,
			FromAddress:          common.HexToAddress("0x33"),
			ToAddress:            common.HexToAddress("0x44"),
			Amount:               big.NewInt(1000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "10",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x55"),
			TokenId:              "1",
			TokenMeta:            "meta",
			TxSignHex:            "0x66",
		},
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567890,
			Status:               TxStatusCreateUnsigned,
			Confirms:             0,
			BlockHash:            common.HexToHash("0x1"),
			BlockNumber:          big.NewInt(1),
			TxHash:               common.HexToHash("0x2"),
			TxType:               TxTypeDeposit,
			FromAddress:          common.HexToAddress("0x3"),
			ToAddress:            common.HexToAddress("0x4"),
			Amount:               big.NewInt(1000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "10",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x5"),
			TokenId:              "1",
			TokenMeta:            "meta",
			TxSignHex:            "0x6",
		},
	}

	// Store initial deposits
	err := depositsDB.StoreDeposits(strconv.Itoa(CurrentRequestId), depositList)
	if err != nil {
		t.Fatalf("failed to store deposits: %v", err)
	}

	// Verify updates
	for _, deposit := range depositList {
		temp, err := depositsDB.QueryDepositsByTxHash(strconv.Itoa(CurrentRequestId), deposit.TxHash)
		if err != nil {
			t.Fatalf("failed to QueryDepositsByTxHash: %v", err)
		}
		t.Logf("QueryDepositsByTxHash 1 %v", json2.ToPrettyJSON(temp))
	}

	// Update the deposits
	newStatus := TxStatusWalletDone
	for _, deposit := range depositList {
		deposit.Status = newStatus
		deposit.Amount = big.NewInt(deposit.Amount.Int64() + 500) // Example update
	}

	err = depositsDB.UpdateDepositListById(strconv.Itoa(CurrentRequestId), depositList)
	if err != nil {
		t.Fatalf("failed to update deposit list: %v", err)
	}

	for _, deposit := range depositList {
		temp, err := depositsDB.QueryDepositsById(strconv.Itoa(CurrentRequestId), deposit.GUID.String())
		if err != nil {
			t.Fatalf("failed to QueryDepositsById: %v", err)
		}
		t.Logf("QueryDepositsById 1 %v", json2.ToPrettyJSON(temp))
	}

	// Update the deposits
	newStatusV2 := TxStatusSuccess
	for _, deposit := range depositList {
		deposit.Status = newStatusV2
		deposit.Amount = big.NewInt(deposit.Amount.Int64() + 10000) // Example update
	}

	err = depositsDB.UpdateDepositListByTxHash(strconv.Itoa(CurrentRequestId), depositList)
	if err != nil {
		t.Fatalf("failed to update deposit list: %v", err)
	}

	for _, deposit := range depositList {
		temp, err := depositsDB.QueryDepositsById(strconv.Itoa(CurrentRequestId), deposit.GUID.String())
		if err != nil {
			t.Fatalf("failed to QueryDepositsById: %v", err)
		}
		t.Logf("QueryDepositsById 1 %v", json2.ToPrettyJSON(temp))
	}
}
