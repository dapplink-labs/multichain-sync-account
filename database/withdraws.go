package database

import (
	"errors"
	"fmt"
	"github.com/dapplink-labs/multichain-sync-account/common/bigint"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type Withdraws struct {
	// 基础信息
	GUID      uuid.UUID `gorm:"primaryKey" json:"guid"`
	Timestamp uint64    `json:"timestamp"`
	Status    TxStatus  `json:"status" gorm:"column:status"`

	// 区块信息
	BlockHash   common.Hash     `gorm:"column:block_hash;serializer:bytes" json:"block_hash"`
	BlockNumber *big.Int        `gorm:"serializer:u256;column:block_number" json:"block_number"`
	TxHash      common.Hash     `gorm:"column:hash;serializer:bytes" json:"hash"`
	TxType      TransactionType `gorm:"column:tx_type" json:"tx_type"`

	// 交易基础信息
	FromAddress common.Address `gorm:"serializer:bytes;column:from_address" json:"from_address"`
	ToAddress   common.Address `gorm:"serializer:bytes;column:to_address" json:"to_address"`
	Amount      *big.Int       `gorm:"serializer:u256;column:amount" json:"amount"`

	// Gas 费用
	GasLimit             uint64 `json:"gas_limit"`
	MaxFeePerGas         string `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas"`

	// Token 相关信息
	TokenType    TokenType      `json:"token_type" gorm:"column:token_type"` // ETH, ERC20, ERC721, ERC1155
	TokenAddress common.Address `json:"token_address" gorm:"serializer:bytes;column:token_address"`
	TokenId      string         `json:"token_id" gorm:"column:token_id"`     // ERC721/ERC1155 的 token ID
	TokenMeta    string         `json:"token_meta" gorm:"column:token_meta"` // Token 元数据

	// 交易签名
	TxSignHex string `json:"tx_sign_hex" gorm:"column:tx_sign_hex"`
}

type WithdrawsView interface {
	QueryNotifyWithdraws(requestId string) ([]*Withdraws, error)
	QueryWithdrawsByHash(requestId string, txHash common.Hash) (*Withdraws, error)
	QueryWithdrawsById(requestId string, guid string) (*Withdraws, error)
	UnSendWithdrawsList(requestId string) ([]*Withdraws, error)
}

type WithdrawsDB interface {
	WithdrawsView

	StoreWithdraw(requestId string, withdraw *Withdraws) error
	UpdateWithdrawByTxHash(requestId string, txHash common.Hash, signedTx string, status TxStatus) error
	UpdateWithdrawById(requestId string, guid string, signedTx string, status TxStatus) error
	UpdateWithdrawStatusById(requestId string, status TxStatus, withdrawsList []*Withdraws) error
	UpdateWithdrawStatusByTxHash(requestId string, status TxStatus, withdrawsList []*Withdraws) error
	UpdateWithdrawListByTxHash(requestId string, withdrawsList []*Withdraws) error
	UpdateWithdrawListById(requestId string, withdrawsList []*Withdraws) error
	HandleFallBackWithdraw(requestId string, startBlock, EndBlock *big.Int) error
}

type withdrawsDB struct {
	gorm *gorm.DB
}

func (db *withdrawsDB) QueryNotifyWithdraws(requestId string) ([]*Withdraws, error) {
	var notifyWithdraws []*Withdraws
	result := db.gorm.Table("withdraws_"+requestId).
		Where("status = ? or status = ?", TxStatusWalletDone, TxStatusNotified).
		Find(&notifyWithdraws)

	if result.Error != nil {
		return nil, fmt.Errorf("query notify withdraws failed: %w", result.Error)
	}

	return notifyWithdraws, nil
}

func (db *withdrawsDB) UnSendWithdrawsList(requestId string) ([]*Withdraws, error) {
	var withdrawsList []*Withdraws
	err := db.gorm.Table("withdraws_"+requestId).
		Where("status = ?", TxStatusSigned).
		Find(&withdrawsList).Error

	if err != nil {
		return nil, fmt.Errorf("query unsend withdraws failed: %w", err)
	}

	return withdrawsList, nil
}

func (db *withdrawsDB) QueryWithdrawsById(requestId string, guid string) (*Withdraws, error) {
	var withdrawsEntity Withdraws
	result := db.gorm.Table("withdraws_"+requestId).Where("guid = ?", guid).Take(&withdrawsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &withdrawsEntity, nil
}

func (db *withdrawsDB) QueryWithdrawsByHash(requestId string, txHash common.Hash) (*Withdraws, error) {
	var withdrawsEntity Withdraws
	result := db.gorm.Table("withdraws_"+requestId).Where("hash = ?", txHash.String()).Take(&withdrawsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &withdrawsEntity, nil
}

func (db *withdrawsDB) UpdateWithdrawByTxHash(requestId string, txHash common.Hash, signedTx string, status TxStatus) error {
	tableName := fmt.Sprintf("withdraws_%s", requestId)

	if err := db.CheckWithdrawExistsByTxHash(tableName, txHash); err != nil {
		return err
	}

	updates := map[string]interface{}{
		"status": status,
	}
	if signedTx != "" {
		updates["tx_sign_hex"] = signedTx
	}

	// 3. 执行更新
	if err := db.gorm.Table(tableName).
		Where("hash = ?", txHash.String()).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update withdraw failed: %w", err)
	}

	// 4. 记录日志
	log.Info("Update withdraw success",
		"requestId", requestId,
		"txHash", txHash.String(),
		"status", status,
		"updates", updates,
	)
	return nil
}

func (db *withdrawsDB) UpdateWithdrawById(requestId string, guid string, signedTx string, status TxStatus) error {
	tableName := fmt.Sprintf("withdraws_%s", requestId)

	if err := db.CheckWithdrawExistsById(tableName, guid); err != nil {
		return err
	}

	updates := map[string]interface{}{
		"status": status,
	}
	if signedTx != "" {
		updates["tx_sign_hex"] = signedTx
	}

	// 3. 执行更新
	if err := db.gorm.Table(tableName).
		Where("guid = ?", guid).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update withdraw failed: %w", err)
	}

	// 4. 记录日志
	log.Info("Update withdraw success",
		"requestId", requestId,
		"guid", guid,
		"status", status,
		"updates", updates,
	)
	return nil
}

func NewWithdrawsDB(db *gorm.DB) WithdrawsDB {
	return &withdrawsDB{gorm: db}
}

func (db *withdrawsDB) StoreWithdraw(requestId string, withdraw *Withdraws) error {
	return db.gorm.Table("withdraws_" + requestId).Create(&withdraw).Error
}

func (db *withdrawsDB) UpdateWithdrawStatusById(requestId string, status TxStatus, withdrawsList []*Withdraws) error {
	if len(withdrawsList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("withdraws_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var guids []uuid.UUID
		for _, withdraw := range withdrawsList {
			guids = append(guids, withdraw.GUID)
		}

		result := tx.Table(tableName).
			Where("guid IN ?", guids).
			Update("status", status)

		if result.Error != nil {
			return fmt.Errorf("batch update status failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			log.Warn("No withdraws updated",
				"requestId", requestId,
				"expectedCount", len(withdrawsList),
			)
		}

		log.Info("Batch update withdraws status success",
			"requestId", requestId,
			"count", result.RowsAffected,
			"status", status,
		)

		return nil
	})
}

func (db *withdrawsDB) UpdateWithdrawStatusByTxHash(requestId string, status TxStatus, withdrawsList []*Withdraws) error {
	if len(withdrawsList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("withdraws_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var txHashList []string
		for _, withdraw := range withdrawsList {
			txHashList = append(txHashList, withdraw.TxHash.String())
		}

		result := tx.Table(tableName).
			Where("hash IN ?", txHashList).
			Update("status", status)

		if result.Error != nil {
			return fmt.Errorf("batch update status failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			log.Warn("No withdraws updated",
				"requestId", requestId,
				"expectedCount", len(withdrawsList),
			)
		}

		log.Info("Batch update withdraws status success",
			"requestId", requestId,
			"count", result.RowsAffected,
			"status", status,
		)

		return nil
	})
}

func (db *withdrawsDB) UpdateWithdrawListByTxHash(requestId string, withdrawsList []*Withdraws) error {
	if len(withdrawsList) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("withdraws_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, withdraw := range withdrawsList {
			// Update each record individually based on TxHash
			result := tx.Table(tableName).
				Where("hash = ?", withdraw.TxHash.String()).
				Updates(map[string]interface{}{
					"status": withdraw.Status,
					"amount": withdraw.Amount,
					// Add other fields to update as necessary
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for TxHash %s: %w", withdraw.TxHash.Hex(), result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				fmt.Printf("No withdraws updated for TxHash: %s\n", withdraw.TxHash.Hex())
			} else {
				// Log success message with the number of rows affected
				fmt.Printf("Updated withdraw for TxHash: %s, status: %s, amount: %s\n", withdraw.TxHash.Hex(), withdraw.Status, withdraw.Amount.String())
			}
		}

		return nil
	})
}

func (db *withdrawsDB) UpdateWithdrawListById(requestId string, withdrawsList []*Withdraws) error {
	if len(withdrawsList) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("withdraws_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, withdraw := range withdrawsList {
			// Update each record individually based on TxHash
			result := tx.Table(tableName).
				Where("guid = ?", withdraw.GUID.String()).
				Updates(map[string]interface{}{
					"status": withdraw.Status,
					"amount": withdraw.Amount,
					"hash":   withdraw.TxHash.String(),
					// Add other fields to update as necessary
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for TxHash %s: %w", withdraw.TxHash.Hex(), result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				fmt.Printf("No withdraws updated for TxHash: %s\n", withdraw.TxHash.Hex())
			} else {
				// Log success message with the number of rows affected
				fmt.Printf("Updated withdraw for TxHash: %s, status: %s, amount: %s\n", withdraw.TxHash.Hex(), withdraw.Status, withdraw.Amount.String())
			}
		}

		return nil
	})
}

func (db *withdrawsDB) CheckWithdrawExistsByTxHash(tableName string, txHash common.Hash) error {
	var exist bool
	err := db.gorm.Table(tableName).
		Where("hash = ?", txHash.String()).
		Select("1").
		Find(&exist).Error

	if err != nil {
		return fmt.Errorf("check withdraw exist failed: %w", err)
	}

	if !exist {
		return fmt.Errorf("withdraw not found: %s", txHash.String())
	}

	return nil
}

func (db *withdrawsDB) CheckWithdrawExistsById(tableName string, id string) error {
	var exist bool
	err := db.gorm.Table(tableName).
		Where("guid = ?", id).
		Select("1").
		Find(&exist).Error

	if err != nil {
		return fmt.Errorf("check withdraw exist failed: %w", err)
	}

	if !exist {
		return fmt.Errorf("withdraw not found: %s", id)
	}

	return nil
}

func (db *withdrawsDB) HandleFallBackWithdraw(requestId string, startBlock, EndBlock *big.Int) error {
	for i := startBlock; i.Cmp(EndBlock) < 0; new(big.Int).Add(i, bigint.One) {
		var withdrawsSingle = Withdraws{}
		result := db.gorm.Table("withdraws" + requestId).Where(&Transactions{BlockNumber: i}).Take(&withdrawsSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		withdrawsSingle.Status = TxStatusFallback
		err := db.gorm.Table("withdraws" + requestId).Save(&withdrawsSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}
