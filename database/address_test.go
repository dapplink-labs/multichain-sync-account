package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapplink-labs/multichain-sync-account/common/json2"
)

func TestAddressesDB_StoreAndQuery(t *testing.T) {
	const (
		CurrentRequestId = 1
		CurrentChainId   = 17000
		CurrentChain     = "ethereum"
	)

	db := SetupDb()
	addressesDB := NewAddressesDB(db.gorm)

	address := &Addresses{
		GUID:        uuid.New(),
		Address:     common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
		AddressType: AddressTypeEOA,
		PublicKey:   "public_key_example",
		Timestamp:   uint64(time.Now().Unix()),
	}

	err := addressesDB.StoreAddresses(strconv.Itoa(CurrentRequestId), []*Addresses{address})
	if err != nil {
		t.Errorf("Failed to store balances: %v", err)
	}

	// Test AddressExist
	exists, addrType := addressesDB.AddressExist(strconv.Itoa(CurrentRequestId), &address.Address)
	assert.True(t, exists)
	assert.Equal(t, AddressTypeEOA, addrType)

	// Test QueryAddressesByToAddress
	result, err := addressesDB.QueryAddressesByToAddress(strconv.Itoa(CurrentRequestId), &address.Address)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	t.Logf("result %v", json2.ToPrettyJSON(result))

	// Test GetAllAddresses
	allAddresses, err := addressesDB.GetAllAddresses(strconv.Itoa(CurrentRequestId))
	assert.NoError(t, err)
	assert.Len(t, allAddresses, 1)
	t.Logf("result %v", json2.ToPrettyJSON(allAddresses))
}

func TestAddressesDB_QueryHotAndColdWalletInfo(t *testing.T) {
	const (
		CurrentRequestId = 1
		CurrentChainId   = 17000
		CurrentChain     = "ethereum"
	)

	db := SetupDb()
	addressesDB := NewAddressesDB(db.gorm)

	hotAddress := &Addresses{
		GUID:        uuid.New(),
		Address:     common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcde1"),
		AddressType: AddressTypeHot,
		PublicKey:   "hot_public_key",
		Timestamp:   uint64(time.Now().Unix()),
	}

	coldAddress := &Addresses{
		GUID:        uuid.New(),
		Address:     common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcde2"),
		AddressType: AddressTypeCold,
		PublicKey:   "cold_public_key",
		Timestamp:   uint64(time.Now().Unix()),
	}

	err := addressesDB.StoreAddresses(strconv.Itoa(CurrentRequestId), []*Addresses{hotAddress, coldAddress})
	assert.NoError(t, err)

	// Test QueryHotWalletInfo
	hotResult, err := addressesDB.QueryHotWalletInfo(strconv.Itoa(CurrentRequestId))
	assert.NoError(t, err)
	assert.NotNil(t, hotResult)
	t.Logf("hotResult %v", json2.ToPrettyJSON(hotResult))

	// Test QueryColdWalletInfo
	coldResult, err := addressesDB.QueryColdWalletInfo(strconv.Itoa(CurrentRequestId))
	assert.NoError(t, err)
	assert.NotNil(t, coldResult)
	t.Logf("coldResult %v", json2.ToPrettyJSON(coldResult))
}
