package database

import (
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
)

func TestStoreBalances(t *testing.T) {
	const (
		CurrentRequestId = 1
		CurrentChainId   = 17000
		CurrentChain     = "ethereum"
	)

	db := SetupDb()
	balancesDB := NewBalancesDB(db.gorm)

	balance := &Balances{
		GUID:         uuid.New(),
		Address:      common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
		TokenAddress: common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdef"),
		AddressType:  "eoa",
		Balance:      big.NewInt(1000),
		LockBalance:  big.NewInt(100),
		Timestamp:    uint64(time.Now().Unix()),
	}

	err := balancesDB.StoreBalances(strconv.Itoa(CurrentRequestId), []*Balances{balance})
	if err != nil {
		t.Errorf("Failed to store balances: %v", err)
	}
}

func TestUpdateBalance(t *testing.T) {
	const (
		CurrentRequestId = 1
		CurrentChainId   = 17000
		CurrentChain     = "ethereum"
	)

	db := SetupDb()
	balancesDB := NewBalancesDB(db.gorm)

	balance := &Balances{
		GUID:         uuid.New(),
		Address:      common.HexToAddress("0x1234567890AbcdEF1234567890aBcdef12345678"),
		TokenAddress: common.HexToAddress("0x0000AbCDeFabcdEfaBcDeFabCDEFAbCDEfABcDeF"),
		AddressType:  "eoa",
		Balance:      big.NewInt(1000),
		LockBalance:  big.NewInt(100),
		Timestamp:    uint64(time.Now().Unix()),
	}

	err := balancesDB.StoreBalances(strconv.Itoa(CurrentRequestId), []*Balances{balance})
	if err != nil {
		t.Errorf("Failed to store balances: %v", err)
	}

	// Update balance
	balance.Balance = big.NewInt(2000)
	err = balancesDB.UpdateBalance(strconv.Itoa(CurrentRequestId), balance)
	if err != nil {
		t.Errorf("Failed to update balance: %v", err)
	}
}

func TestQueryWalletBalanceByTokenAndAddress(t *testing.T) {
	const (
		CurrentRequestId = 1
		CurrentChainId   = 17000
		CurrentChain     = "ethereum"
	)

	db := SetupDb()
	balancesDB := NewBalancesDB(db.gorm)

	address := common.HexToAddress("0x1234567890AbcdEF1234567890aBcdef12345678")
	tokenAddress := common.HexToAddress("0x0000AbCDeFabcdEfaBcDeFabCDEFAbCDEfABcDeF")

	// Query non-existing balance
	_, err := balancesDB.QueryWalletBalanceByTokenAndAddress(strconv.Itoa(CurrentRequestId), "eoa", address, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error for non-existing balance, got %v", err)
	}

	// Create initial balance
	balance, err := balancesDB.QueryWalletBalanceByTokenAndAddress(strconv.Itoa(CurrentRequestId), "eoa", address, tokenAddress)
	if err != nil {
		t.Errorf("Failed to create initial balance: %v", err)
	}

	if balance.Address != address {
		t.Errorf("Expected address %v, got %v", address, balance.Address)
	}

	t.Logf("balance %v", json2.ToPrettyJSON(balance))
}
