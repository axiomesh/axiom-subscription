package client

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/axiomesh/axiom-subscription/internal/dao"
	"github.com/axiomesh/axiom-subscription/internal/model"
	subTypes "github.com/axiomesh/axiom-subscription/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jmoiron/sqlx"
)

type ClientInterface interface {
	GetAllSubscription() []int
	RemoveSubsciption(subId int) error
	AddSubsciption(tag string, addresses []common.Address, topics [][]common.Hash, handler func(brl *subTypes.BlockRangeLogs) (err error)) (int, error)
}

type ClientImpl struct {
	ChainId         int64
	rpcUrl          string
	wsUrl           string
	SubscriptionDao *dao.SubscriptionDao
	rpcClient       *ethclient.Client
	wsClinet        *ethclient.Client
	Subscriptions   []*subTypes.Subscription

	ctx    context.Context
	cancel context.CancelFunc
}

func NewClient(rpcUrl string, wsUrl string, db *sqlx.DB) (*ClientImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())
	rpcClient, err := ethclient.Dial(rpcUrl)
	if err != nil {
		return nil, err
	}
	wsClinet, err := ethclient.Dial(wsUrl)
	if err != nil {
		return nil, err
	}
	chainId, err := rpcClient.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	sd, err := dao.NewSubscriptionDao(db)
	if err != nil {
		return nil, err
	}
	var subscriptions []*subTypes.Subscription
	client := ClientImpl{
		Subscriptions:   subscriptions,
		ChainId:         chainId.Int64(),
		SubscriptionDao: sd,
		rpcClient:       rpcClient,
		wsClinet:        wsClinet,
		rpcUrl:          rpcUrl,
		wsUrl:           wsUrl,
		ctx:             ctx,
		cancel:          cancel,
	}
	headers := make(chan *types.Header)
	sub, err := client.wsClinet.SubscribeNewHead(ctx, headers)
	if err != nil {
		return nil, err
	}
	go client.ListenAndHandle(headers, sub)
	return &client, nil
}

func (c *ClientImpl) ListenAndHandle(headers chan *types.Header, sub ethereum.Subscription) {
	for {
		select {
		case err := <-sub.Err():
			if err != nil {
				fmt.Println(err)
				sub, err = c.reconnect(headers, sub)
				if err != nil || sub == nil {
					fmt.Println(err)
				} else {
					break
				}
			}
		case header := <-headers:
			fmt.Println(header.Number)
			for _, sub := range c.Subscriptions {
				startNum := sub.Height.Add(sub.Height, big.NewInt(1))
				if startNum.Cmp(header.Number) <= 0 {
					logs, err := c.rpcClient.FilterLogs(c.ctx, ethereum.FilterQuery{
						FromBlock: startNum,
						ToBlock:   header.Number,
						Addresses: sub.Addresses,
						Topics:    sub.Topics,
					})
					if err != nil {
						fmt.Print(err)
					}
					if len(logs) == 0 {
						sub.Height = header.Number
						subModel, err := subTypes.ToSubscriptionModel(sub)
						if err != nil {
							continue
						}
						err = c.SubscriptionDao.UpdateHeight(c.ctx, subModel)
						if err != nil {
							continue
						}
					} else {
						blockRangeSub := &subTypes.BlockRangeLogs{
							ChainID: sub.ChainId,
							Logs:    logs,
							Start:   startNum,
							End:     header.Number,
						}
						err = sub.Handler(blockRangeSub)
						if err != nil {
							fmt.Println(err)
							continue
						}
						sub.Height = header.Number
						subModel, err := subTypes.ToSubscriptionModel(sub)
						if err != nil {
							continue
						}
						err = c.SubscriptionDao.UpdateHeight(c.ctx, subModel)
						if err != nil {
							continue
						}
					}

				}

			}
		}
	}
}

func (c *ClientImpl) reconnect(headers chan *types.Header, sub ethereum.Subscription) (ethereum.Subscription, error) {
	heartbeatInterval := 15 * time.Second
	heartbeatTimer := time.NewTicker(heartbeatInterval)
	for range heartbeatTimer.C {
		newClient, err := ethclient.Dial(c.wsUrl)
		if err == nil {
			c.wsClinet.Close()
			c.wsClinet = newClient
			break
		}
	}
	sub.Unsubscribe()
	// Attempt to reconnect
	newSub, err := c.wsClinet.SubscribeNewHead(c.ctx, headers)
	if err != nil {
		return nil, err
	}
	return newSub, nil
}

func (c *ClientImpl) AddSubsciption(tag string, addresses []common.Address, topics [][]common.Hash, handler func(brl *subTypes.BlockRangeLogs) (err error)) (int, error) {
	currentHeigeht, err := c.rpcClient.BlockNumber(c.ctx)
	if err != nil {
		return 0, err
	}
	subs, err := c.SubscriptionDao.QueryByChainIdAndTag(c.ctx, int(c.ChainId), tag)
	if err != nil {
		return 0, err
	}
	if len(subs) > 1 {
		return 0, errors.New("more than one subscription with same tag")
	}
	if len(subs) != 0 {
		for _, sub := range c.Subscriptions {
			if subs[0].Tag == sub.Tag {
				return sub.Id, nil
			}
		}
		subsubTypes, err := subTypes.FromSubscriptionModel(subs[0])
		if err != nil {
			return subsubTypes.Id, err
		}
		subsubTypes.Handler = handler
		c.Subscriptions = append(c.Subscriptions, subsubTypes)
		return subsubTypes.Id, nil
	} else {
		sub := &subTypes.Subscription{
			ChainId:   int(c.ChainId),
			Tag:       tag,
			Start:     big.NewInt(int64(currentHeigeht)),
			Height:    big.NewInt(int64(currentHeigeht)),
			Addresses: addresses,
			Topics:    topics,
			Handler:   handler,
		}
		subModel, err := subTypes.ToSubscriptionModel(sub)
		if err != nil {
			return subModel.ID, err
		}
		subId, err := c.SubscriptionDao.InsertSubscription(c.ctx, subModel)
		sub.Id = subId
		c.Subscriptions = append(c.Subscriptions, sub)
		return subId, nil
	}
}

func (c *ClientImpl) RemoveSubsciption(subId int) error {
	err := c.SubscriptionDao.DeleteSubscription(c.ctx, &model.Subscription{ID: subId})
	if err != nil {
		return err
	}
	var newSubscriptions []*subTypes.Subscription
	for _, sub := range c.Subscriptions {
		if sub.Id != subId {
			newSubscriptions = append(newSubscriptions, sub)
		}
	}
	c.Subscriptions = newSubscriptions
	return nil
}

func (c *ClientImpl) GetSubscriptionStartAndHeight(subId int) (*big.Int, *big.Int, error) {
	for _, sub := range c.Subscriptions {
		if sub.Id == subId {
			return sub.Start, sub.Height, nil
		}
	}
	return nil, nil, errors.New("subscription not found")
}

func (c *ClientImpl) GetAllSubscription() []int {
	var ids []int
	for _, sub := range c.Subscriptions {
		ids = append(ids, sub.Id)
	}
	return ids
}
