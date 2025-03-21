package rpcclient

import (
	"errors"
	"fmt"

	"math/big"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/bigint"
)

var (
	ErrBatchBlockAheadOfProvider = errors.New("the BatchBlock's internal state is ahead of the provider")
)

type BatchBlock struct {
	rpcClient *WalletChainAccountClient

	latestHeader        *BlockHeader
	lastTraversedHeader *BlockHeader

	blockConfirmationDepth *big.Int
}

func NewBatchBlock(rpcClient *WalletChainAccountClient, fromHeader *BlockHeader, confDepth *big.Int) *BatchBlock {
	return &BatchBlock{
		rpcClient:              rpcClient,
		lastTraversedHeader:    fromHeader,
		blockConfirmationDepth: confDepth,
	}
}

func (f *BatchBlock) LatestHeader() *BlockHeader {
	return f.latestHeader
}

func (f *BatchBlock) LastTraversedHeader() *BlockHeader {
	return f.lastTraversedHeader
}

func (f *BatchBlock) NextHeaders(maxSize uint64) ([]BlockHeader, *BlockHeader, bool, error) {
	latestHeader, err := f.rpcClient.GetBlockHeader(nil)
	if err != nil {
		return nil, nil, false, fmt.Errorf("unable to query latest block: %w", err)
	} else if latestHeader == nil {
		return nil, nil, false, fmt.Errorf("latest header unreported")
	} else {
		f.latestHeader = latestHeader
	}
	endHeight := new(big.Int).Sub(latestHeader.Number, f.blockConfirmationDepth)
	if endHeight.Sign() < 0 {
		return nil, nil, false, nil
	}
	if f.lastTraversedHeader != nil {
		cmp := f.lastTraversedHeader.Number.Cmp(endHeight)
		if cmp == 0 {
			return nil, nil, false, nil
		} else if cmp > 0 {
			return nil, nil, false, ErrBatchBlockAheadOfProvider
		}
	}
	nextHeight := bigint.Zero
	if f.lastTraversedHeader != nil {
		nextHeight = new(big.Int).Add(f.lastTraversedHeader.Number, bigint.One)
	}
	endHeight = bigint.Clamp(nextHeight, endHeight, maxSize)
	count := new(big.Int).Sub(endHeight, nextHeight).Uint64() + 1
	headers := make([]BlockHeader, count)
	for i := uint64(0); i < count; i++ {
		height := new(big.Int).Add(nextHeight, new(big.Int).SetUint64(i))
		blockHeader, err := f.rpcClient.GetBlockHeader(height)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return nil, nil, false, err
		}
		if headers != nil && i-1 > 0 {
			if blockHeader.Number.Cmp(headers[i-1].Number) < 0 || blockHeader.ParentHash != headers[i-1].Hash {
				headers[i] = *blockHeader
				return headers, blockHeader, true, err
			}
		}
		headers[i] = *blockHeader
	}

	numHeaders := len(headers)
	if numHeaders == 0 {
		return nil, nil, false, nil
	}

	f.lastTraversedHeader = &headers[numHeaders-1]
	return headers, nil, false, nil
}
