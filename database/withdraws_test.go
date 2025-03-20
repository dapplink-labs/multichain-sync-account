package database

import (
	"math/big"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
)

func TestQueryNotifyWithdraws(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	withdraw := &Withdraws{
		GUID:                 uuid.New(),
		Timestamp:            1234567890,
		Status:               TxStatusWalletDone,
		BlockHash:            common.HexToHash("0x1"),
		BlockNumber:          big.NewInt(1),
		TxHash:               common.HexToHash("0x2"),
		FromAddress:          common.HexToAddress("0x3"),
		ToAddress:            common.HexToAddress("0x4"),
		Amount:               big.NewInt(1000),
		GasLimit:             21000,
		MaxFeePerGas:         "100",
		MaxPriorityFeePerGas: "2",
		TokenType:            TokenTypeERC20,
		TokenAddress:         common.HexToAddress("0x5"),
		TokenId:              "1",
		TokenMeta:            "meta",
		TxSignHex:            "0x6",
	}

	err := withdrawsDB.StoreWithdraw(requestId, withdraw)
	if err != nil {
		t.Fatalf("failed to store withdraw: %v", err)
	}

	notifyWithdraws, err := withdrawsDB.QueryNotifyWithdraws(requestId)
	if err != nil {
		t.Fatalf("failed to query notify withdraws: %v", err)
	}
	t.Logf("notifyWithdraws %v", json2.ToPrettyJSON(notifyWithdraws))
}

func TestUnSendWithdrawsList(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	withdraw := &Withdraws{
		GUID:                 uuid.New(),
		Timestamp:            1234567890,
		Status:               TxStatusSigned,
		BlockHash:            common.HexToHash("0x1"),
		BlockNumber:          big.NewInt(1),
		TxHash:               common.HexToHash("0x2"),
		FromAddress:          common.HexToAddress("0x3"),
		ToAddress:            common.HexToAddress("0x4"),
		Amount:               big.NewInt(1000),
		GasLimit:             21000,
		MaxFeePerGas:         "100",
		MaxPriorityFeePerGas: "2",
		TokenType:            TokenTypeERC20,
		TokenAddress:         common.HexToAddress("0x5"),
		TokenId:              "1",
		TokenMeta:            "meta",
		TxSignHex:            "0x6",
	}

	err := withdrawsDB.StoreWithdraw(requestId, withdraw)
	if err != nil {
		t.Fatalf("failed to store withdraw: %v", err)
	}

	unSendWithdraws, err := withdrawsDB.UnSendWithdrawsList(requestId)
	if err != nil {
		t.Fatalf("failed to query unsend withdraws list: %v", err)
	}
	t.Logf("unSendWithdraws %v", json2.ToPrettyJSON(unSendWithdraws))
}

func TestQueryWithdrawsByHash(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	withdraw := &Withdraws{
		GUID:                 uuid.New(),
		Timestamp:            1234567890,
		Status:               TxStatusSigned,
		BlockHash:            common.HexToHash("0x1"),
		BlockNumber:          big.NewInt(1),
		TxHash:               common.HexToHash("0x2"),
		FromAddress:          common.HexToAddress("0x3"),
		ToAddress:            common.HexToAddress("0x4"),
		Amount:               big.NewInt(1000),
		GasLimit:             21000,
		MaxFeePerGas:         "100",
		MaxPriorityFeePerGas: "2",
		TokenType:            TokenTypeERC20,
		TokenAddress:         common.HexToAddress("0x5"),
		TokenId:              "1",
		TokenMeta:            "meta",
		TxSignHex:            "0x6",
	}

	err := withdrawsDB.StoreWithdraw(requestId, withdraw)
	if err != nil {
		t.Fatalf("failed to store withdraw: %v", err)
	}

	retrievedWithdraw, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
	if err != nil {
		t.Fatalf("failed to query withdraw by hash: %v", err)
	}
	t.Logf("retrievedWithdraw %v", json2.ToPrettyJSON(retrievedWithdraw))
}

func TestUpdateWithdrawTx(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	withdraw := &Withdraws{
		GUID:                 uuid.New(),
		Timestamp:            1234567890,
		Status:               TxStatusWalletDone,
		BlockHash:            common.HexToHash("0x1"),
		BlockNumber:          big.NewInt(1),
		TxHash:               common.HexToHash("0x2"),
		FromAddress:          common.HexToAddress("0x3"),
		ToAddress:            common.HexToAddress("0x4"),
		Amount:               big.NewInt(1000),
		GasLimit:             21000,
		MaxFeePerGas:         "100",
		MaxPriorityFeePerGas: "2",
		TokenType:            TokenTypeERC20,
		TokenAddress:         common.HexToAddress("0x5"),
		TokenId:              "1",
		TokenMeta:            "meta",
		TxSignHex:            "0x6",
	}
	err := withdrawsDB.StoreWithdraw(requestId, withdraw)
	if err != nil {
		t.Fatalf("failed to store withdraw: %v", err)
	}
	updatedWithdraw, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated withdraw: %v", err)
	}
	t.Logf("updatedWithdraw 1 %v", json2.ToPrettyJSON(updatedWithdraw))

	newStatus := TxStatusSigned
	signedTx := "0x7"
	err = withdrawsDB.UpdateWithdrawByTxHash(requestId, withdraw.TxHash, signedTx, newStatus)
	if err != nil {
		t.Fatalf("failed to update withdraw tx: %v", err)
	}

	updatedWithdrawV2, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated withdraw: %v", err)
	}
	t.Logf("updatedWithdraw 2 %v", json2.ToPrettyJSON(updatedWithdrawV2))
}

func TestUpdateWithdrawStatus(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	withdraw := &Withdraws{
		GUID:                 uuid.New(),
		Timestamp:            1234567890,
		Status:               TxStatusWalletDone,
		BlockHash:            common.HexToHash("0x1"),
		BlockNumber:          big.NewInt(1),
		TxHash:               common.HexToHash("0x2"),
		FromAddress:          common.HexToAddress("0x3"),
		ToAddress:            common.HexToAddress("0x4"),
		Amount:               big.NewInt(1000),
		GasLimit:             21000,
		MaxFeePerGas:         "100",
		MaxPriorityFeePerGas: "2",
		TokenType:            TokenTypeERC20,
		TokenAddress:         common.HexToAddress("0x5"),
		TokenId:              "1",
		TokenMeta:            "meta",
		TxSignHex:            "0x6",
	}
	err := withdrawsDB.StoreWithdraw(requestId, withdraw)
	if err != nil {
		t.Fatalf("failed to store withdraw: %v", err)
	}

	updatedWithdraw, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated withdraw: %v", err)
	}
	t.Logf("updatedWithdraw 1 %v", json2.ToPrettyJSON(updatedWithdraw))

	newStatus := TxStatusSigned
	signedTx := "0x7"
	err = withdrawsDB.UpdateWithdrawByTxHash(requestId, withdraw.TxHash, signedTx, newStatus)
	if err != nil {
		t.Fatalf("failed to update withdraw tx: %v", err)
	}

	updatedWithdrawV2, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated withdraw: %v", err)
	}
	t.Logf("updatedWithdraw 2 %v", json2.ToPrettyJSON(updatedWithdrawV2))
}

func TestUpdateWithdrawList(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	withdrawsDB := NewWithdrawsDB(db.gorm)
	requestId := strconv.Itoa(CurrentRequestId)

	// Initial withdraws to be stored
	withdrawsList := []*Withdraws{
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567890,
			Status:               TxStatusWalletDone,
			BlockHash:            common.HexToHash("0x1"),
			BlockNumber:          big.NewInt(1),
			TxHash:               common.HexToHash("0x2"),
			FromAddress:          common.HexToAddress("0x3"),
			ToAddress:            common.HexToAddress("0x4"),
			Amount:               big.NewInt(1000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "2",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x5"),
			TokenId:              "1",
			TokenMeta:            "meta",
			TxSignHex:            "0x6",
		},
		{
			GUID:                 uuid.New(),
			Timestamp:            1234567891,
			Status:               TxStatusWalletDone,
			BlockHash:            common.HexToHash("0x1"),
			BlockNumber:          big.NewInt(2),
			TxHash:               common.HexToHash("0x3"),
			FromAddress:          common.HexToAddress("0x3"),
			ToAddress:            common.HexToAddress("0x4"),
			Amount:               big.NewInt(2000),
			GasLimit:             21000,
			MaxFeePerGas:         "100",
			MaxPriorityFeePerGas: "2",
			TokenType:            TokenTypeERC20,
			TokenAddress:         common.HexToAddress("0x5"),
			TokenId:              "2",
			TokenMeta:            "meta",
			TxSignHex:            "0x7",
		},
	}

	// Store initial withdraws
	for _, withdraw := range withdrawsList {
		err := withdrawsDB.StoreWithdraw(requestId, withdraw)
		if err != nil {
			t.Fatalf("failed to store withdraw: %v", err)
		}
	}

	// Verify updates
	for _, withdraw := range withdrawsList {
		updatedWithdraw, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
		if err != nil {
			t.Fatalf("failed to query updated withdraw: %v", err)
		}
		t.Logf("updatedWithdraw 1 %v", json2.ToPrettyJSON(updatedWithdraw))
	}

	// Update the withdrawals
	newStatus := TxStatusSigned
	for _, withdraw := range withdrawsList {
		withdraw.Status = newStatus
	}

	err := withdrawsDB.UpdateWithdrawListByTxHash(requestId, withdrawsList)
	if err != nil {
		t.Fatalf("failed to update withdraw list: %v", err)
	}

	// Verify updates
	for _, withdraw := range withdrawsList {
		updatedWithdraw, err := withdrawsDB.QueryWithdrawsByHash(requestId, withdraw.TxHash)
		if err != nil {
			t.Fatalf("failed to query updated withdraw: %v", err)
		}
		t.Logf("updatedWithdraw 2 %v", json2.ToPrettyJSON(updatedWithdraw))
	}
}
