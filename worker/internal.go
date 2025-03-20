package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-account/common/retry"
	"github.com/dapplink-labs/multichain-sync-account/common/tasks"
	"github.com/dapplink-labs/multichain-sync-account/config"
	"github.com/dapplink-labs/multichain-sync-account/database"
	"github.com/dapplink-labs/multichain-sync-account/rpcclient"
)

type Internal struct {
	rpcClient      *rpcclient.WalletChainAccountClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewInternal(cfg *config.Config, db *database.DB, rpcClient *rpcclient.WalletChainAccountClient, shutdown context.CancelCauseFunc) (*Internal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &Internal{
		rpcClient:      rpcClient,
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in internals: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
}

func (w *Internal) Close() error {
	var result error
	w.resourceCancel()
	w.ticker.Stop()
	log.Info("stop internal......")
	if err := w.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await internal %w", err))
		return result
	}
	log.Info("stop internal success")
	return nil
}

func (w *Internal) Start() error {
	log.Info("start internals......")
	w.tasks.Go(func() error {
		for {
			select {
			case <-w.ticker.C:
				log.Info("collection and hot to cold")
				businessList, err := w.db.Business.QueryBusinessList()
				if err != nil {
					log.Error("query business list fail", "err", err)
					continue
				}

				for _, businessId := range businessList {
					unSendTransactionList, err := w.db.Internals.UnSendInternalsList(businessId.BusinessUid)
					if err != nil {
						log.Error("query un send internal tx list fail", "err", err)
						continue
					}
					if len(unSendTransactionList) == 0 {
						log.Error("Withdraw Start", "businessId", businessId, "unSendTransactionList", "is null")
						continue
					}

					var balanceList []*database.Balances

					for _, unSendInternalTx := range unSendTransactionList {
						txHash, err := w.rpcClient.SendTx(unSendInternalTx.TxSignHex)
						if err != nil {
							log.Error("send transaction fail", "err", err)
							continue
						} else {
							balanceItem := &database.Balances{
								TokenAddress: unSendInternalTx.TokenAddress,
								Address:      unSendInternalTx.FromAddress,
								LockBalance:  unSendInternalTx.Amount,
							}
							balanceList = append(balanceList, balanceItem)

							unSendInternalTx.TxHash = common.HexToHash(txHash)
							unSendInternalTx.Status = database.TxStatusBroadcasted
						}
					}

					retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
					if _, err := retry.Do[interface{}](w.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
						if err := w.db.Transaction(func(tx *database.DB) error {
							if len(balanceList) > 0 {
								log.Info("Update address balance", "totalTx", len(balanceList))
								if err := tx.Balances.UpdateBalanceListByTwoAddress(businessId.BusinessUid, balanceList); err != nil {
									log.Error("Update address balance fail", "err", err)
									return err
								}

							}
							if len(unSendTransactionList) > 0 {
								err = w.db.Internals.UpdateInternalListById(businessId.BusinessUid, unSendTransactionList)
								if err != nil {
									log.Error("update internals status fail", "err", err)
									return err
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
				}
			case <-w.resourceCtx.Done():
				log.Info("stop internals in worker")
				return nil
			}
		}
	})
	return nil
}
