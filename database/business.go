package database

import (
	"errors"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Business struct {
	GUID        uuid.UUID `gorm:"primaryKey" json:"guid"`
	BusinessUid string    `json:"business_uid"`
	NotifyUrl   string    `json:"notify_url"`
	Timestamp   uint64
}

type BusinessView interface {
	QueryBusinessList() ([]*Business, error)
	QueryBusinessByUuid(string) (*Business, error)
}

type BusinessDB interface {
	BusinessView

	StoreBusiness(*Business) error
}

type businessDB struct {
	gorm *gorm.DB
}

func NewBusinessDB(db *gorm.DB) BusinessDB {
	return &businessDB{gorm: db}
}

func (db *businessDB) StoreBusiness(business *Business) error {
	result := db.gorm.Table("business").Create(business)
	return result.Error
}

func (db *businessDB) QueryBusinessList() ([]*Business, error) {
	var business []*Business
	err := db.gorm.Table("business").Find(&business).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}
	return business, err
}

func (db *businessDB) QueryBusinessByUuid(businessUid string) (*Business, error) {
	var business *Business
	result := db.gorm.Table("business").Where("business_uid", businessUid).First(&business)
	if result.Error != nil {
		log.Error("query business all fail", "Err", result.Error)
		return nil, result.Error
	}
	return business, nil
}
