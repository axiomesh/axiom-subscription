package dao

import (
	"context"

	"github.com/axiomesh/axiom-subscription/internal/model"
	"github.com/jmoiron/sqlx"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

type SubscriptionDao struct {
	db *sqlx.DB
}

func NewSubscriptionDao(db *sqlx.DB) (*SubscriptionDao, error) {
	sd := &SubscriptionDao{db: db}
	err := sd.createTableIfNotExists()
	if err != nil {
		return nil, err
	}
	return sd, nil
}

func (dao *SubscriptionDao) createTableIfNotExists() error {
	// 检查表格是否存在的 SQL 查询语句
	query := `SELECT EXISTS (
                    SELECT 1
                    FROM   information_schema.tables 
                    WHERE  table_schema = 'public'
                    AND    table_name = 'subscription'
                )`

	var exists bool
	err := dao.db.QueryRow(query).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		createTableQuery := `CREATE TABLE subscription
		(
			id serial PRIMARY KEY,
			chain_id int NOT NULL,
			tag  varchar(50) NOT NULL,
			addresses varchar(500) NOT NULL,
			topics  varchar(500) NOT NULL,
			start text NOT NULL,
			height text NOT NULL,
			created_at timestamp DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamp DEFAULT CURRENT_TIMESTAMP
		);`
		_, err = dao.db.Exec(createTableQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dao *SubscriptionDao) QueryByChainIdAndTag(ctx context.Context, chainId int, tag string) ([]*model.Subscription, error) {
	subscriptions, err := model.Subscriptions(qm.Where("chain_id = ? AND tag = ?", chainId, tag)).All(ctx, dao.db)
	if err != nil {
		return nil, err
	}
	return subscriptions, nil
}

func (dao *SubscriptionDao) InsertSubscription(ctx context.Context, sub *model.Subscription) (int, error) {
	err := sub.Insert(ctx, dao.db, boil.Infer())
	if err != nil {
		return sub.ID, err
	}
	return sub.ID, nil
}

func (dao *SubscriptionDao) UpdateHeight(ctx context.Context, sub *model.Subscription) error {
	_, err := sub.Update(ctx, dao.db, boil.Whitelist(model.SubscriptionColumns.Height))
	if err != nil {
		return err
	}
	return nil
}

func (dao *SubscriptionDao) DeleteSubscription(ctx context.Context, sub *model.Subscription) error {
	_, err := sub.Delete(ctx, dao.db)
	if err != nil {
		return err
	}
	return nil
}
