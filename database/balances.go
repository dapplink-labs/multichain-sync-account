package database

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Balances struct {
	GUID         uuid.UUID      `gorm:"primary_key" json:"guid"`
	Address      common.Address `gorm:"type:varchar;not null;serializer:bytes" json:"address"`
	TokenAddress common.Address `gorm:"type:varchar;not null;serializer:bytes" json:"token_address"`
	AddressType  AddressType    `gorm:"type:varchar(10);not null;default:'eoa'" json:"address_type"`
	Balance      *big.Int       `gorm:"type:numeric;not null;default:0;check:balance >= 0;serializer:u256" json:"balance"`
	LockBalance  *big.Int       `gorm:"type:numeric;not null;default:0;serializer:u256" json:"lock_balance"`
	Timestamp    uint64         `gorm:"type:bigint;not null;check:timestamp > 0" json:"timestamp"`
}

type BalancesView interface {
	QueryWalletBalanceByTokenAndAddress(
		requestId string,
		addressType AddressType,
		address,
		tokenAddress common.Address,
	) (*Balances, error)
}

type BalancesDB interface {
	BalancesView

	UpdateOrCreate(string, []*TokenBalance) error
	UpdateFallBackBalance(string, []*TokenBalance) error
	StoreBalances(string, []*Balances) error
	UpdateBalanceListByTwoAddress(string, []*Balances) error
	UpdateBalance(string, *Balances) error
}

type balancesDB struct {
	gorm *gorm.DB
}

func NewBalancesDB(db *gorm.DB) BalancesDB {
	return &balancesDB{gorm: db}
}

func (db *balancesDB) StoreBalances(requestId string, balanceList []*Balances) error {
	valueList := make([]Balances, len(balanceList))
	for i, balance := range balanceList {
		if balance != nil {
			balance.Address = common.HexToAddress(balance.Address.Hex())
			balance.TokenAddress = common.HexToAddress(balance.TokenAddress.Hex())
			valueList[i] = *balance
		}
	}
	return db.gorm.Table("balances_"+requestId).CreateInBatches(&valueList, len(valueList)).Error
}

func (db *balancesDB) UpdateBalance(requestId string, balance *Balances) error {
	if balance == nil {
		return fmt.Errorf("balance cannot be nil")
	}

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		return db.UpdateAndSaveBalance(tx, requestId, balance)
	})
}

func (db *balancesDB) UpdateAndSaveBalance(tx *gorm.DB, requestId string, balance *Balances) error {
	if balance == nil {
		return fmt.Errorf("balance cannot be nil")
	}

	var currentBalance Balances
	result := tx.Table("balances_"+requestId).
		Where("address = ? AND token_address = ?",
			strings.ToLower(balance.Address.String()),
			strings.ToLower(balance.TokenAddress.String()),
		).
		Take(&currentBalance)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Debug("Balance record not found",
				"requestId", requestId,
				"address", balance.Address.String(),
				"tokenAddress", balance.TokenAddress.String())
			return nil
		}
		return fmt.Errorf("query balance failed: %w", result.Error)
	}

	currentBalance.Balance = balance.Balance //上游修改这里不做重复计算
	currentBalance.LockBalance = new(big.Int).Add(currentBalance.LockBalance, balance.LockBalance)
	currentBalance.Timestamp = uint64(time.Now().Unix())

	if err := tx.Table("balances_" + requestId).Save(&currentBalance).Error; err != nil {
		log.Error("Failed to save balance",
			"requestId", requestId,
			"address", balance.Address.String(),
			"error", err)
		return fmt.Errorf("save balance failed: %w", err)
	}

	log.Debug("Balance updated and saved successfully",
		"requestId", requestId,
		"address", balance.Address.String(),
		"tokenAddress", balance.TokenAddress.String(),
		"newBalance", currentBalance.Balance.String(),
		"lockBalance", currentBalance.LockBalance.String())

	return nil
}

func (db *balancesDB) UpdateBalanceListByTwoAddress(requestId string, balanceList []*Balances) error {
	if len(balanceList) == 0 {
		return nil
	}

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, balance := range balanceList {
			var currentBalance Balances
			result := tx.Table("balances_"+requestId).
				Where("address = ? AND token_address = ?",
					balance.Address.String(),
					balance.TokenAddress.String()).
				Take(&currentBalance)

			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					continue
				}
				return fmt.Errorf("query balance failed: %w", result.Error)
			}

			currentBalance.Balance = new(big.Int).Sub(currentBalance.Balance, balance.LockBalance)
			currentBalance.LockBalance = balance.LockBalance
			currentBalance.Timestamp = uint64(time.Now().Unix())

			if err := tx.Table("balances_" + requestId).Save(&currentBalance).Error; err != nil {
				return fmt.Errorf("save balance failed: %w", err)
			}
		}
		return nil
	})
}

func (db *balancesDB) QueryWalletBalanceByTokenAndAddress(
	requestId string,
	addressType AddressType,
	address,
	tokenAddress common.Address,
) (*Balances, error) {
	balance, err := db.queryBalance(requestId, address, tokenAddress)
	if err == nil {
		return balance, nil
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return db.createInitialBalance(requestId, addressType, address, tokenAddress)
	}

	return nil, fmt.Errorf("query balance failed: %w", err)
}

func (db *balancesDB) queryBalance(
	requestId string,
	address,
	tokenAddress common.Address,
) (*Balances, error) {
	var balance Balances

	err := db.gorm.Table("balances_"+requestId).
		Where("address = ? AND token_address = ?",
			strings.ToLower(address.String()),
			strings.ToLower(tokenAddress.String()),
		).
		Take(&balance).
		Error

	if err != nil {
		return nil, err
	}

	return &balance, nil
}

func (db *balancesDB) createInitialBalance(
	requestId string,
	addressType AddressType,
	address,
	tokenAddress common.Address,
) (*Balances, error) {
	balance := &Balances{
		GUID:         uuid.New(),
		Address:      address,
		TokenAddress: tokenAddress,
		AddressType:  addressType,
		Balance:      big.NewInt(0),
		LockBalance:  big.NewInt(0),
		Timestamp:    uint64(time.Now().Unix()),
	}

	if err := db.gorm.Table("balances_" + requestId).Create(balance).Error; err != nil {
		log.Error("Failed to create initial balance",
			"requestId", requestId,
			"address", address.String(),
			"tokenAddress", tokenAddress.String(),
			"error", err,
		)
		return nil, fmt.Errorf("create initial balance failed: %w", err)
	}

	log.Debug("Created initial balance",
		"requestId", requestId,
		"address", address.String(),
		"tokenAddress", tokenAddress.String(),
	)

	return balance, nil
}

func (db *balancesDB) UpdateOrCreate(requestId string, balanceList []*TokenBalance) error {
	if len(balanceList) == 0 {
		return nil
	}
	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, balance := range balanceList {
			log.Info("Processing balance update",
				"txType", balance.TxType,
				"from", balance.FromAddress,
				"to", balance.ToAddress,
				"token", balance.TokenAddress,
				"amount", balance.Balance)

			if err := db.handleBalanceUpdate(tx, requestId, balance); err != nil {
				return fmt.Errorf("failed to handle balance update: %w", err)
			}
		}
		return nil
	})
}

func (db *balancesDB) UpdateFallBackBalance(requestId string, balanceList []*TokenBalance) error {
	if len(balanceList) == 0 {
		return nil
	}
	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, balance := range balanceList {
			switch balance.TxType {
			case TxTypeDeposit:
				return db.handleFallBackDeposit(tx, requestId, balance)
			case TxTypeWithdraw:
				return db.handleFallBackWithdraw(tx, requestId, balance)
			case TxTypeCollection:
				return db.handleFallBackCollection(tx, requestId, balance)
			case TxTypeHot2Cold:
				return db.handleFallBackHotToCold(tx, requestId, balance)
			case TxTypeCold2Hot:
				return db.handleFallBackColdToHot(tx, requestId, balance)
			default:
				return fmt.Errorf("unsupported transaction type: %s", balance.TxType)
			}
		}
		return nil
	})
}

func (db *balancesDB) handleBalanceUpdate(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	switch balance.TxType {
	case TxTypeDeposit:
		return db.handleDeposit(tx, requestId, balance)
	case TxTypeWithdraw:
		return db.handleWithdraw(tx, requestId, balance)
	case TxTypeCollection:
		return db.handleCollection(tx, requestId, balance)
	case TxTypeHot2Cold:
		return db.handleHotToCold(tx, requestId, balance)
	case TxTypeCold2Hot:
		return db.handleColdToHot(tx, requestId, balance)
	default:
		return fmt.Errorf("unsupported transaction type: %s", balance.TxType)
	}
}

func (db *balancesDB) handleDeposit(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	userAddress, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeUser, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query user address failed", "err", err)
		return err
	}
	log.Info("Processing handleDeposit",
		"txType", balance.TxType,
		"from", balance.FromAddress,
		"to", balance.ToAddress,
		"token", balance.TokenAddress,
		"amount", balance.Balance,
		"userAddress.Balance,", userAddress.Balance)
	userAddress.Balance = new(big.Int).Add(userAddress.Balance, balance.Balance)
	log.Info("userAddress.Balance after", new(big.Int).Add(userAddress.Balance, balance.Balance))
	return db.UpdateAndSaveBalance(tx, requestId, userAddress)
}

func (db *balancesDB) handleFallBackDeposit(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	userAddress, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeUser, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query user address failed", "err", err)
		return err
	}
	log.Info("Processing handleDeposit",
		"txType", balance.TxType,
		"from", balance.FromAddress,
		"to", balance.ToAddress,
		"token", balance.TokenAddress,
		"amount", balance.Balance,
		"userAddress.Balance,", userAddress.Balance)
	userAddress.Balance = new(big.Int).Sub(userAddress.Balance, balance.Balance)
	log.Info("userAddress.Balance after", new(big.Int).Sub(userAddress.Balance, balance.Balance))
	return db.UpdateAndSaveBalance(tx, requestId, userAddress)
}

func (db *balancesDB) handleWithdraw(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}

	hotWallet.Balance = new(big.Int).Sub(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}

func (db *balancesDB) handleFallBackWithdraw(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}

	hotWallet.Balance = new(big.Int).Add(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}

func (db *balancesDB) handleCollection(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	userWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeUser, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query user wallet failed", "err", err)
		return err
	}
	userWallet.Balance = new(big.Int).Sub(userWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, userWallet); err != nil {
		return err
	}

	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Add(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}

func (db *balancesDB) handleFallBackCollection(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	userWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeUser, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query user wallet failed", "err", err)
		return err
	}
	userWallet.Balance = new(big.Int).Add(userWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, userWallet); err != nil {
		return err
	}

	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Sub(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}

func (db *balancesDB) handleHotToCold(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Sub(hotWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, hotWallet); err != nil {
		return err
	}

	coldWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeCold, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query cold wallet failed", "err", err)
		return err
	}
	coldWallet.Balance = new(big.Int).Add(coldWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, coldWallet)
}

func (db *balancesDB) handleFallBackHotToCold(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Add(hotWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, hotWallet); err != nil {
		return err
	}

	coldWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeCold, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query cold wallet failed", "err", err)
		return err
	}
	coldWallet.Balance = new(big.Int).Sub(coldWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, coldWallet)
}

func (db *balancesDB) handleColdToHot(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	coldWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeCold, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query cold wallet failed", "err", err)
		return err
	}
	coldWallet.Balance = new(big.Int).Sub(coldWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, coldWallet); err != nil {
		return err
	}

	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Add(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}

func (db *balancesDB) handleFallBackColdToHot(tx *gorm.DB, requestId string, balance *TokenBalance) error {
	coldWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeCold, balance.ToAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query cold wallet failed", "err", err)
		return err
	}
	coldWallet.Balance = new(big.Int).Add(coldWallet.Balance, balance.Balance)
	if err := db.UpdateAndSaveBalance(tx, requestId, coldWallet); err != nil {
		return err
	}

	hotWallet, err := db.QueryWalletBalanceByTokenAndAddress(requestId, AddressTypeHot, balance.FromAddress, balance.TokenAddress)
	if err != nil {
		log.Error("Query hot wallet failed", "err", err)
		return err
	}
	hotWallet.Balance = new(big.Int).Sub(hotWallet.Balance, balance.Balance)
	return db.UpdateAndSaveBalance(tx, requestId, hotWallet)
}
