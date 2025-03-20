package database

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Addresses struct {
	GUID        uuid.UUID      `gorm:"primary_key" json:"guid"`
	Address     common.Address `gorm:"type:varchar;unique;not null;serializer:bytes" json:"address"`
	AddressType AddressType    `gorm:"type:varchar(10);not null;default:'user'" json:"address_type"`
	PublicKey   string         `gorm:"type:varchar;not null" json:"public_key"`
	Timestamp   uint64         `gorm:"type:bigint;not null;check:timestamp > 0" json:"timestamp"`
}

type AddressesView interface {
	AddressExist(requestId string, address *common.Address) (bool, AddressType)
	QueryAddressesByToAddress(string, *common.Address) (*Addresses, error)
	QueryHotWalletInfo(string) (*Addresses, error)
	QueryColdWalletInfo(string) (*Addresses, error)
	GetAllAddresses(string) ([]*Addresses, error)
}

type AddressesDB interface {
	AddressesView

	StoreAddresses(string, []*Addresses) error
}

func NewAddressesDB(db *gorm.DB) AddressesDB {
	return &addressesDB{gorm: db}
}

type addressesDB struct {
	gorm *gorm.DB
}

func (db *addressesDB) AddressExist(requestId string, address *common.Address) (bool, AddressType) {
	var addressEntry Addresses
	err := db.gorm.Table("addresses_"+requestId).
		Where("address = ?", strings.ToLower(address.String())).
		First(&addressEntry).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, AddressTypeUser
		}
		return false, AddressTypeUser
	}
	return true, addressEntry.AddressType
}

func (db *addressesDB) QueryAddressesByToAddress(requestId string, address *common.Address) (*Addresses, error) {
	var addressEntry Addresses
	err := db.gorm.Table("addresses_"+requestId).
		Where("address = ?", strings.ToLower(address.String())).
		Take(&addressEntry).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}
	return &addressEntry, nil
}

// StoreAddresses store address
func (db *addressesDB) StoreAddresses(requestId string, addressList []*Addresses) error {
	for _, addr := range addressList {
		addr.Address = common.HexToAddress(addr.Address.Hex())
	}

	return db.gorm.Table("addresses_"+requestId).
		CreateInBatches(&addressList, len(addressList)).Error
}

func (db *addressesDB) QueryHotWalletInfo(requestId string) (*Addresses, error) {
	var addressEntry Addresses
	err := db.gorm.Table("addresses_"+requestId).
		Where("address_type = ?", AddressTypeHot).
		Take(&addressEntry).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &addressEntry, nil
}

func (db *addressesDB) QueryColdWalletInfo(requestId string) (*Addresses, error) {
	var addressEntry Addresses
	err := db.gorm.Table("addresses_"+requestId).
		Where("address_type = ?", AddressTypeCold).
		Take(&addressEntry).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &addressEntry, nil
}

func (db *addressesDB) GetAllAddresses(requestId string) ([]*Addresses, error) {
	var addresses []*Addresses
	err := db.gorm.Table("addresses_" + requestId).Find(&addresses).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return addresses, nil
}

func (a *Addresses) Validate() error {
	if a.Address == (common.Address{}) {
		return errors.New("invalid address")
	}
	if a.PublicKey == "" {
		return errors.New("invalid public key")
	}
	if a.Timestamp == 0 {
		return errors.New("invalid timestamp")
	}
	switch a.AddressType {
	case AddressTypeUser, AddressTypeHot, AddressTypeCold:
		return nil
	default:
		return errors.New("invalid address type")
	}
}
