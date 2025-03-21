package database

import (
	"errors"
	"fmt"
	"github.com/dapplink-labs/multichain-sync-account/common/bigint"
	"math/big"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type Deposits struct {
	GUID      uuid.UUID `gorm:"primaryKey;type:varchar(36)" json:"guid"`
	Timestamp uint64    `gorm:"not null;check:timestamp > 0" json:"timestamp"`
	Status    TxStatus  `gorm:"type:varchar(10);not null" json:"status"`
	Confirms  uint8     `gorm:"not null;default:0" json:"confirms"`

	BlockHash   common.Hash     `gorm:"type:varchar;not null;serializer:bytes" json:"block_hash"`
	BlockNumber *big.Int        `gorm:"not null;check:block_number > 0;serializer:u256" json:"block_number"`
	TxHash      common.Hash     `gorm:"column:hash;type:varchar;not null;serializer:bytes" json:"hash"`
	TxType      TransactionType `gorm:"type:varchar;not null" json:"tx_type"`

	FromAddress common.Address `gorm:"type:varchar;not null;serializer:bytes" json:"from_address"`
	ToAddress   common.Address `gorm:"type:varchar;not null;serializer:bytes" json:"to_address"`
	Amount      *big.Int       `gorm:"not null;serializer:u256" json:"amount"`

	GasLimit             uint64 `gorm:"not null" json:"gas_limit"`
	MaxFeePerGas         string `gorm:"type:varchar;not null" json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `gorm:"type:varchar;not null" json:"max_priority_fee_per_gas"`

	TokenType    TokenType      `gorm:"type:varchar;not null" json:"token_type"`
	TokenAddress common.Address `gorm:"type:varchar;not null;serializer:bytes" json:"token_address"`
	TokenId      string         `gorm:"type:varchar;not null" json:"token_id"`
	TokenMeta    string         `gorm:"type:varchar;not null" json:"token_meta"`

	TxSignHex string `gorm:"type:varchar;not null" json:"tx_sign_hex"`
}

type DepositsView interface {
	QueryNotifyDeposits(requestId string) ([]*Deposits, error)
	QueryDepositsByTxHash(requestId string, txHash common.Hash) (*Deposits, error)
	QueryDepositsById(requestId string, guid string) (*Deposits, error)
}

type DepositsDB interface {
	DepositsView

	StoreDeposits(string, []*Deposits) error
	UpdateDepositsComfirms(requestId string, blockNumber uint64, confirms uint64) error
	UpdateDepositById(requestId string, guid string, signedTx string, status TxStatus) error
	UpdateDepositsStatusById(requestId string, status TxStatus, depositList []*Deposits) error
	UpdateDepositsStatusByTxHash(requestId string, status TxStatus, depositList []*Deposits) error
	UpdateDepositListByTxHash(requestId string, depositList []*Deposits) error
	UpdateDepositListById(requestId string, depositList []*Deposits) error
	HandleFallBackDeposits(requestId string, startBlock, EndBlock *big.Int) error
}

type depositsDB struct {
	gorm *gorm.DB
}

func (db *depositsDB) QueryNotifyDeposits(requestId string) ([]*Deposits, error) {
	var notifyDeposits []*Deposits
	result := db.gorm.Table("deposits_"+requestId).
		Where("status = ? or status = ?", TxStatusWalletDone, TxStatusNotified).
		Find(&notifyDeposits) // Correctly populate the slice
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil // Return nil slice instead of error
		}
		return nil, result.Error
	}
	return notifyDeposits, nil
}

func (db *depositsDB) QueryDepositsByTxHash(requestId string, txHash common.Hash) (*Deposits, error) {
	var deposit Deposits
	result := db.gorm.Table("deposits_"+requestId).
		Where("hash = ?", txHash.String()).
		Take(&deposit)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil // Return nil if no record is found
		}
		return nil, result.Error
	}

	return &deposit, nil
}

func (db *depositsDB) QueryDepositsById(requestId string, guid string) (*Deposits, error) {
	var deposit Deposits
	result := db.gorm.Table("deposits_"+requestId).
		Where("guid = ?", guid).
		Take(&deposit)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil // Return nil if no record is found
		}
		return nil, result.Error
	}

	return &deposit, nil
}

// UpdateDepositsComfirms 查询所有还没有过确认位交易，用最新区块减去对应区块更新确认，如果这个大于我们预设的确认位，那么这笔交易可以认为已经入账
func (db *depositsDB) UpdateDepositsComfirms(requestId string, blockNumber uint64, confirms uint64) error {
	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var unConfirmDeposits []*Deposits
		result := tx.Table("deposits_"+requestId).
			Where("block_number <= ? AND status = ?", blockNumber, TxStatusBroadcasted).
			Find(&unConfirmDeposits)
		if result.Error != nil {
			return result.Error
		}

		for _, deposit := range unConfirmDeposits {
			chainConfirm := blockNumber - deposit.BlockNumber.Uint64()
			if chainConfirm >= confirms {
				deposit.Confirms = uint8(confirms)
				deposit.Status = TxStatusWalletDone
			} else {
				deposit.Confirms = uint8(chainConfirm)
			}
			if err := tx.Table("deposits_" + requestId).Save(&deposit).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *depositsDB) UpdateDepositsStatusById(requestId string, status TxStatus, depositList []*Deposits) error {
	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, deposit := range depositList {
			var depositSingle Deposits
			result := tx.Table("deposits_"+requestId).Where("guid = ?", deposit.GUID.String()).Take(&depositSingle)
			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					continue // Skip if not found
				}
				return result.Error
			}
			depositSingle.Status = status
			if err := tx.Table("deposits_" + requestId).Save(&depositSingle).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *depositsDB) UpdateDepositsStatusByTxHash(requestId string, status TxStatus, depositList []*Deposits) error {
	if len(depositList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("deposits_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var txHashList []string
		for _, deposit := range depositList {
			txHashList = append(txHashList, deposit.TxHash.String())
		}

		result := tx.Table(tableName).
			Where("hash IN ?", txHashList).
			Update("status", status)

		if result.Error != nil {
			return fmt.Errorf("batch update status failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			log.Warn("No deposits updated",
				"requestId", requestId,
				"expectedCount", len(depositList),
			)
		}

		log.Info("Batch update deposits status success",
			"requestId", requestId,
			"count", result.RowsAffected,
			"status", status,
		)

		return nil
	})
}

func (db *depositsDB) UpdateDepositListByTxHash(requestId string, depositList []*Deposits) error {
	if len(depositList) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("deposits_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, deposit := range depositList {
			// Update each record individually based on TxHash
			result := tx.Table(tableName).
				Where("hash = ?", deposit.TxHash.String()).
				Updates(map[string]interface{}{
					"status": deposit.Status,
					"amount": deposit.Amount,
					// Add other fields to update as necessary
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for TxHash %s: %w", deposit.TxHash.String(), result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				fmt.Printf("No deposits updated for TxHash: %s\n", deposit.TxHash.Hex())
			} else {
				// Log success message with the number of rows affected
				fmt.Printf("Updated deposit for TxHash: %s, status: %s, amount: %s\n", deposit.TxHash.Hex(), deposit.Status, deposit.Amount.String())
			}
		}

		return nil
	})
}

func (db *depositsDB) UpdateDepositById(requestId string, guid string, signedTx string, status TxStatus) error {
	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var deposit Deposits
		result := tx.Table("deposits_"+requestId).
			Where("guid = ?", guid).
			Take(&deposit)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("deposit not found for GUID: %s", guid)
			}
			return result.Error
		}

		deposit.Status = status
		deposit.TxSignHex = signedTx

		if err := tx.Table("deposits_" + requestId).Save(&deposit).Error; err != nil {
			return fmt.Errorf("failed to update deposit for GUID: %s, error: %w", guid, err)
		}

		return nil
	})
}

func NewDepositsDB(db *gorm.DB) DepositsDB {
	return &depositsDB{gorm: db}
}

func (db *depositsDB) StoreDeposits(requestId string, depositList []*Deposits) error {
	if len(depositList) == 0 {
		return nil
	}
	result := db.gorm.Table("deposits_"+requestId).CreateInBatches(depositList, len(depositList))
	if result.Error != nil {
		log.Error("create deposit batch fail", "Err", result.Error)
		return result.Error
	}
	return nil
}

func (db *depositsDB) UpdateDepositListById(requestId string, depositList []*Deposits) error {
	if len(depositList) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("deposits_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, deposit := range depositList {
			// Update each record individually based on GUID
			result := tx.Table(tableName).
				Where("guid = ?", deposit.GUID.String()).
				Updates(map[string]interface{}{
					"status": deposit.Status,
					"amount": deposit.Amount,
					"hash":   deposit.TxHash.String(),
					// Add other fields to update as necessary
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for GUID %s: %w", deposit.GUID.String(), result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				fmt.Printf("No deposits updated for GUID: %s\n", deposit.GUID.String())
			} else {
				// Log success message with the number of rows affected
				fmt.Printf("Updated deposit for GUID: %s, status: %s, amount: %s\n", deposit.GUID.String(), deposit.Status, deposit.Amount.String())
			}
		}

		return nil
	})
}

func (db *depositsDB) HandleFallBackDeposits(requestId string, startBlock, EndBlock *big.Int) error {
	for i := startBlock; i.Cmp(EndBlock) < 0; new(big.Int).Add(i, bigint.One) {
		var depositsSingle = Deposits{}
		result := db.gorm.Table("deposits" + requestId).Where(&Transactions{BlockNumber: i}).Take(&depositsSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		depositsSingle.Status = TxStatusFallback
		err := db.gorm.Table("deposits" + requestId).Save(&depositsSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}
