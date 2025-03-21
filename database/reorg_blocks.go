package database

import (
	"errors"
	"math/big"

	"gorm.io/gorm"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
)

type ReorgBlocks struct {
	Hash       common.Hash `gorm:"primaryKey;serializer:bytes"`
	ParentHash common.Hash `gorm:"serializer:bytes"`
	Number     *big.Int    `gorm:"serializer:u256"`
	Timestamp  uint64
}

func ReorgBlockHeaderFromHeader(header *types.Header) rpcclient.BlockHeader {
	return rpcclient.BlockHeader{
		Hash:       header.Hash(),
		ParentHash: header.ParentHash,
		Number:     header.Number,
		Timestamp:  header.Time,
	}
}

type ReorgBlocksView interface {
	LatestReorgBlocks() (*rpcclient.BlockHeader, error)
}

type ReorgBlocksDB interface {
	ReorgBlocksView

	StoreReorgBlocks([]ReorgBlocks) error
}

type reorgBlocksDB struct {
	gorm *gorm.DB
}

func NewReorgBlocksDB(db *gorm.DB) ReorgBlocksDB {
	return &reorgBlocksDB{gorm: db}
}

func (db *reorgBlocksDB) StoreReorgBlocks(headers []ReorgBlocks) error {
	result := db.gorm.CreateInBatches(&headers, len(headers))
	return result.Error
}

func (db *reorgBlocksDB) LatestReorgBlocks() (*rpcclient.BlockHeader, error) {
	var header ReorgBlocks
	result := db.gorm.Order("number DESC").Take(&header)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return (*rpcclient.BlockHeader)(&header), nil
}
