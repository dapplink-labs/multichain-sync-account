package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/bigint"
	"github.com/dapplink-labs/multichain-sync-account/common/retry"
	"github.com/dapplink-labs/multichain-sync-account/common/tasks"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
)

type FallBack struct {
	deposit        *Deposit
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewFallBack(cfg *config.Config, deposit *Deposit, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &FallBack{
		deposit:        deposit,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in fallback: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
}

func (fb *FallBack) Close() error {
	var result error
	fb.resourceCancel()
	fb.ticker.Stop()
	log.Info("stop fallback......")
	if err := fb.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await fallback %w", err))
		return result
	}
	log.Info("stop fallback success")
	return nil
}

/*
 * 加一张：reorg_block
 * 触发 fallback 的逻辑有两个
 * -- 当前区块的 ParentHash != 上一个区块的 Hash
 * -- 当前链上的最新块高 < 数据库里面的最新块高
 * 处理流程
 * -- 扫链任务里面检测到回滚的条件，通知 fallback 执行回滚
 * -- 将回滚或者重组的区块全部放到 reorg_block
 * -- 将所有对应 reorg_block 的交易状态修改成回滚状态（处理的是没有过确认位的交易）
 */

func (fb *FallBack) Start() error {
	log.Info("start fallback......")
	fb.tasks.Go(func() error {
		for {
			select {
			case <-fb.ticker.C:
				log.Info("fallback task")
				syncUpdates := fb.deposit.Notify()
				for range syncUpdates {
					log.Info("notified of fallback", "fallbackBlockNumber", fb.deposit.fallbackBlockHeader.Number)
					if err := fb.onFallBack(fb.deposit.fallbackBlockHeader); err != nil {
						log.Error("handle fallback block fail", "err", err)
					}
				}
				log.Info("no block fallback, shutting down fallback task")
				return nil
			case <-fb.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}

func (fb *FallBack) onFallBack(fallbackBlockHeader *rpcclient.BlockHeader) error {
	var reorgBlockHeader []database.ReorgBlocks
	lastBlockHeader := fallbackBlockHeader
	// 充值交易：把用户地址上的资金减掉
	// 提现交易：把热钱包地址上的资金加上
	// 归集(用户地址到热钱包)：把热钱包地址上的资金减掉，把用户地址上的资金加上
	// 热转温(用户地址到热钱包)：把热钱包地址上的资金加上，把温钱包地址上的资金减掉
	// 温转热(用户地址到热钱包)：把温钱包地址上的资金加上，把热钱包地址上的资金加上
	// var balances []*database.FbTokenAddressBalance
	// 文件 mock 数据，然后进行
	// - 先 mock 1000
	// - 让 900-1000 的 Hash 发生成
	for {
		reorgBlockHeader = append(reorgBlockHeader, database.ReorgBlocks{
			Hash:       lastBlockHeader.Hash,
			ParentHash: lastBlockHeader.ParentHash,
			Number:     lastBlockHeader.Number,
			Timestamp:  lastBlockHeader.Timestamp,
		})

		lastBlockNumber := new(big.Int).Sub(lastBlockHeader.Number, bigint.One)
		chainBlockHeader, err := fb.deposit.rpcClient.GetBlockHeader(lastBlockNumber)
		if err != nil {
			return err
		}
		if lastBlockHeader.ParentHash == chainBlockHeader.Hash {
			break
		}
		lastBlockHeader = chainBlockHeader
	}

	businessList, err := fb.deposit.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("Query business list fail", "err", err)
		return err
	}
	var fallbackBalances []*database.TokenBalance
	for _, businessItem := range businessList {
		transactionsList, err := fb.deposit.database.Transactions.QueryFallBackTransactions(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number)
		if err != nil {
			return err
		}
		for _, transaction := range transactionsList {
			fbb := &database.TokenBalance{
				FromAddress:  transaction.FromAddress,
				ToAddress:    transaction.ToAddress,
				TokenAddress: transaction.TokenAddress,
				Balance:      new(big.Int).Neg(transaction.Amount),
				TxType:       transaction.TxType,
			}
			fallbackBalances = append(fallbackBalances, fbb)
		}
	}

	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](fb.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := fb.deposit.database.Transaction(func(tx *database.DB) error {
			if len(reorgBlockHeader) > 0 {
				log.Info("Store deposit transaction success", "totalTx", len(reorgBlockHeader))
				if err := tx.ReorgBlocks.StoreReorgBlocks(reorgBlockHeader); err != nil {
					return err
				}
			}
			if fallbackBlockHeader.Number.Cmp(lastBlockHeader.Number) > 0 {
				for _, businessItem := range businessList {
					if err := tx.Deposits.HandleFallBackDeposits(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Internals.HandleFallBackInternals(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Withdraws.HandleFallBackWithdraw(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Transactions.HandleFallBackTransactions(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Balances.UpdateOrCreate(businessItem.BusinessUid, fallbackBalances); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			log.Error("unable to persist batch", "err", err)
			return nil, err
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return nil
}
