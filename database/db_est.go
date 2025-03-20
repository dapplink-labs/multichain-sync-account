package database

import (
	"context"

	"github.com/dapplink-labs/multichain-sync-account/config"
)

func SetupDb() *DB {
	dbConfig := config.DbConfigTest()

	newDB, _ := NewDB(context.Background(), *dbConfig)
	return newDB
}
