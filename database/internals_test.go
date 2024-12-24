package database

import (
	"math/big"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
)

func TestQueryNotifyInternal(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId
	internal := &Internals{
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
		TxSignHex:            "",
	}

	err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
	if err != nil {
		t.Fatalf("failed to store internal: %v", err)
	}

	notifyInternals, err := internalsDB.QueryNotifyInternal(strconv.Itoa(requestId))
	if err != nil {
		t.Fatalf("failed to query notify internals: %v", err)
	}
	t.Logf("notifyInternals %v", json2.ToPrettyJSON(notifyInternals))
}

func TestStoreInternal(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId
	internal := &Internals{
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

	err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
	if err != nil {
		t.Fatalf("failed to store internal: %v", err)
	}

	storedInternal, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
	if err != nil {
		t.Fatalf("failed to query stored internal: %v", err)
	}
	t.Logf("notifyInternals %v", json2.ToPrettyJSON(storedInternal))
}

func TestUnSendInternalsList(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId
	internal := &Internals{
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

	err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
	if err != nil {
		t.Fatalf("failed to store internal: %v", err)
	}

	unSendInternals, err := internalsDB.UnSendInternalsList(strconv.Itoa(requestId))
	if err != nil {
		t.Fatalf("failed to query unsend internals list: %v", err)
	}
	t.Logf("unSendInternals %v", json2.ToPrettyJSON(unSendInternals))
}

func TestUpdateInternalTx(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId
	internal := &Internals{
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

	err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
	if err != nil {
		t.Fatalf("failed to store internal: %v", err)
	}
	updatedInternal, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated internal: %v", err)
	}
	t.Logf("updatedInternal 1 %v", json2.ToPrettyJSON(updatedInternal))

	newStatus := TxStatusSigned
	signedTx := "0x7"
	err = internalsDB.UpdateInternalByTxHash(strconv.Itoa(requestId), internal.TxHash, signedTx, newStatus)
	if err != nil {
		t.Fatalf("failed to update internal tx: %v", err)
	}

	updatedInternalV2, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated internal: %v", err)
	}
	t.Logf("updatedInternal 2 %v", json2.ToPrettyJSON(updatedInternalV2))
}

func TestUpdateInternalStatus(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId
	internal := &Internals{
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

	err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
	if err != nil {
		t.Fatalf("failed to store internal: %v", err)
	}
	updatedInternal, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated internal: %v", err)
	}
	t.Logf("updatedInternal 1 %v", json2.ToPrettyJSON(updatedInternal))

	newStatus := TxStatusSigned
	err = internalsDB.UpdateInternalStatusByTxHash(strconv.Itoa(requestId), newStatus, []*Internals{internal})
	if err != nil {
		t.Fatalf("failed to update internal tx: %v", err)
	}

	updatedInternalV2, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
	if err != nil {
		t.Fatalf("failed to query updated internal: %v", err)
	}
	t.Logf("updatedInternal 2 %v", json2.ToPrettyJSON(updatedInternalV2))
}

func TestUpdateInternalList(t *testing.T) {
	const (
		CurrentRequestId = 1
	)

	db := SetupDb()
	internalsDB := NewInternalsDB(db.gorm)

	requestId := CurrentRequestId

	// Initial internals to be stored
	internalsList := []*Internals{
		{
			GUID:        uuid.New(),
			Timestamp:   1234567890,
			Status:      TxStatusWalletDone,
			BlockHash:   common.HexToHash("0x1"),
			BlockNumber: big.NewInt(1),
			TxHash:      common.HexToHash("0x2"),
			FromAddress: common.HexToAddress("0x3"),
			ToAddress:   common.HexToAddress("0x4"),
			Amount:      big.NewInt(1000),
		},
		{
			GUID:        uuid.New(),
			Timestamp:   1234567891,
			Status:      TxStatusWalletDone,
			BlockHash:   common.HexToHash("0x1"),
			BlockNumber: big.NewInt(2),
			TxHash:      common.HexToHash("0x3"),
			FromAddress: common.HexToAddress("0x3"),
			ToAddress:   common.HexToAddress("0x4"),
			Amount:      big.NewInt(2000),
		},
	}

	// Store initial internals
	for _, internal := range internalsList {
		err := internalsDB.StoreInternal(strconv.Itoa(requestId), internal)
		if err != nil {
			t.Fatalf("failed to store internal: %v", err)
		}
	}

	// Verify updates
	for _, internal := range internalsList {
		updatedInternal, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
		if err != nil {
			t.Fatalf("failed to query updated internal: %v", err)
		}
		t.Logf("updatedInternal 1 %v", json2.ToPrettyJSON(updatedInternal))
	}

	// Update the internals
	newStatus := TxStatusSigned
	for _, internal := range internalsList {
		internal.Status = newStatus
		internal.Amount = big.NewInt(internal.Amount.Int64() + 500) // Example update
	}

	err := internalsDB.UpdateInternalListByHash(strconv.Itoa(requestId), internalsList)
	if err != nil {
		t.Fatalf("failed to update internal list: %v", err)
	}

	// Verify updates
	for _, internal := range internalsList {
		updatedInternal, err := internalsDB.QueryInternalsByTxHash(strconv.Itoa(requestId), internal.TxHash)
		if err != nil {
			t.Fatalf("failed to query updated internal: %v", err)
		}
		if updatedInternal.Amount.Cmp(big.NewInt(internal.Amount.Int64())) != 0 {
			t.Errorf("expected amount %s, got %s", internal.Amount.String(), updatedInternal.Amount.String())
		}
		t.Logf("updatedInternal 2 %v", json2.ToPrettyJSON(updatedInternal))
	}
}
