package database

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type TokenBalance struct {
	FromAddress  common.Address  `json:"from_address"`
	ToAddress    common.Address  `json:"to_address"`
	TokenAddress common.Address  `json:"to_ken_address"`
	Balance      *big.Int        `json:"balance"`
	TxType       TransactionType `json:"tx_type"`
}
