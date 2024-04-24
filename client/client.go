package client

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/axiomesh/axiom-subscription/internal/dao"
	subTypes "github.com/axiomesh/axiom-subscription/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jmoiron/sqlx"
)

type Client interface {
	AddSubscription(tag string, addresses []common.Address, topics [][]common.Hash, isPersisted bool, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error
	RemoveSubsciption(tag string) error
	GetSubscriptionStartAndHeight(tag string) (*big.Int, *big.Int, error)
	GetSubscriptionTags() []string
}

type SubClient struct {
	ChainId         int64
	rpcUrl          string
	wsUrl           string
	SubscriptionDao *dao.SubscriptionDao
	rpcClient       *ethclient.Client
	wsClinet        *ethclient.Client
	Subscriptions   map[string]*subTypes.Subscription

	ctx context.Context
}

func NewClient(rpcUrl string, wsUrl string, db *sqlx.DB) (*SubClient, error) {
	ctx := context.Background()
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
	client := SubClient{
		Subscriptions:   make(map[string]*subTypes.Subscription),
		ChainId:         chainId.Int64(),
		SubscriptionDao: sd,
		rpcClient:       rpcClient,
		wsClinet:        wsClinet,
		rpcUrl:          rpcUrl,
		wsUrl:           wsUrl,
		ctx:             ctx,
	}
	headers := make(chan *types.Header)
	sub, err := client.wsClinet.SubscribeNewHead(ctx, headers)
	if err != nil {
		return nil, err
	}
	go client.ListenAndHandle(headers, sub)
	return &client, nil
}

func (c *SubClient) ListenAndHandle(headers chan *types.Header, sub ethereum.Subscription) {
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
						if sub.IsPersisted {
							subModel, err := subTypes.ToSubscriptionModel(sub)
							if err != nil {
								continue
							}
							err = c.SubscriptionDao.UpdateHeight(c.ctx, subModel)
							if err != nil {
								continue
							}
						}
					} else {
						blockRangeSub := &subTypes.BlockRangeLogs{
							ChainId: sub.ChainId,
							Logs:    logs,
							Start:   startNum,
							End:     header.Number,
						}
						err = sub.Handler(subTypes.NewSubClientCtx(c.ctx), blockRangeSub)
						if err != nil {
							continue
						}
						sub.Height = header.Number
						if sub.IsPersisted {
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
}

func (c *SubClient) reconnect(headers chan *types.Header, sub ethereum.Subscription) (ethereum.Subscription, error) {
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
	newSub, err := c.wsClinet.SubscribeNewHead(c.ctx, headers)
	if err != nil {
		return nil, err
	}
	return newSub, nil
}

func (c *SubClient) AddSubscription(tag string, addresses []common.Address, topics [][]common.Hash, isPersisted bool, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error {
	if _, ok := c.Subscriptions[tag]; ok {
		return nil
	}
	subTag, err := c.SubscriptionDao.QueryByChainIdAndTag(c.ctx, int(c.ChainId), tag)
	if err == nil && subTag != nil && !isPersisted {
		return errors.New("persisted subscription already exists")
	}

	if !isPersisted {
		currentHeigeht, err := c.rpcClient.BlockNumber(c.ctx)
		if err != nil {
			return err
		}
		sub := &subTypes.Subscription{
			ChainId:     int(c.ChainId),
			Tag:         tag,
			Start:       big.NewInt(int64(currentHeigeht)),
			Height:      big.NewInt(int64(currentHeigeht)),
			Addresses:   addresses,
			Topics:      topics,
			IsPersisted: isPersisted,
			Handler:     handler,
		}
		c.Subscriptions[tag] = sub
		return nil
	}

	if err == nil && subTag != nil {
		subFromDb, err := subTypes.FromSubscriptionModel(subTag)
		if err != nil {
			return err
		}
		if !compareAddresses(subFromDb.Addresses, addresses) || !compareTopics(subFromDb.Topics, topics) {
			return errors.New("err Subscription Data")
		}
		subFromDb.Handler = handler
		subFromDb.IsPersisted = isPersisted
		c.Subscriptions[tag] = subFromDb
	} else {
		currentHeigeht, err := c.rpcClient.BlockNumber(c.ctx)
		if err != nil {
			return err
		}
		sub := &subTypes.Subscription{
			ChainId:     int(c.ChainId),
			Tag:         tag,
			Start:       big.NewInt(int64(currentHeigeht)),
			Height:      big.NewInt(int64(currentHeigeht)),
			Addresses:   addresses,
			Topics:      topics,
			IsPersisted: isPersisted,
			Handler:     handler,
		}
		subModel, err := subTypes.ToSubscriptionModel(sub)
		if err != nil {
			return err
		}
		subId, err := c.SubscriptionDao.InsertSubscription(c.ctx, subModel)
		sub.Id = subId
		c.Subscriptions[tag] = sub
	}
	return nil
}

func (c *SubClient) RemoveSubsciption(tag string) error {
	err := c.SubscriptionDao.DeleteSubscription(c.ctx, tag, int(c.ChainId))
	if err != nil {
		return err
	}
	c.Subscriptions[tag] = nil
	return nil
}

func (c *SubClient) GetSubscriptionStartAndHeight(tag string) (*big.Int, *big.Int, error) {
	for _, sub := range c.Subscriptions {
		if sub.Tag == tag {
			return sub.Start, sub.Height, nil
		}
	}
	return nil, nil, errors.New("subscription not found")
}

func (c *SubClient) GetSubscriptionTags() []string {
	var tags []string
	for _, sub := range c.Subscriptions {
		tags = append(tags, sub.Tag)
	}
	return tags
}

func compareTopics(slice1, slice2 [][]common.Hash) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i := range slice1 {
		if len(slice1[i]) != len(slice2[i]) {
			return false
		}
		for j := range slice1[i] {
			if slice1[i][j] != slice2[i][j] {
				return false
			}
		}
	}

	return true
}

func compareAddresses(slice1, slice2 []common.Address) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}

	return true
}
