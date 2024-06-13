package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

const LostLogsQueryRange = 1999

type Client interface {
	AddSubscription(tag string, addresses []common.Address, topics [][]common.Hash, isPersisted bool, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error
	AddSubscriptionFromHeight(tag string, addresses []common.Address, topics [][]common.Hash, isPersisted bool, height *big.Int, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error
	RemoveSubsciption(tag string) error
	GetSubscriptionStartAndHeight(tag string) (*big.Int, *big.Int, error)
	GetSubscriptionTags() []string
	GetLogsHistory(addresses []common.Address, topics [][]common.Hash, start *big.Int, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error
	GetChainId() int64
}

type SubClient struct {
	chainId         int64
	rpcUrl          string
	wsUrl           string
	subscriptionDao *dao.SubscriptionDao
	rpcClient       *ethclient.Client
	wsClinet        *ethclient.Client
	subscriptions   map[string]*subTypes.Subscription
	persistSupport  bool

	logger slog.Logger
	ctx    context.Context
}

func NewCacheClient(rpcUrl string, wsUrl string, logger slog.Logger) (*SubClient, error) {
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
	client := SubClient{
		subscriptions:  make(map[string]*subTypes.Subscription),
		chainId:        chainId.Int64(),
		rpcClient:      rpcClient,
		wsClinet:       wsClinet,
		rpcUrl:         rpcUrl,
		wsUrl:          wsUrl,
		persistSupport: false,
		logger:         logger,
		ctx:            ctx,
	}
	headers := make(chan *types.Header)
	sub, err := client.wsClinet.SubscribeNewHead(ctx, headers)
	if err != nil {
		return nil, err
	}
	go client.ListenAndHandle(headers, sub)
	return &client, nil
}

func NewClient(rpcUrl string, wsUrl string, db *sqlx.DB, logger slog.Logger) (*SubClient, error) {
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
		subscriptions:   make(map[string]*subTypes.Subscription),
		chainId:         chainId.Int64(),
		subscriptionDao: sd,
		rpcClient:       rpcClient,
		wsClinet:        wsClinet,
		rpcUrl:          rpcUrl,
		wsUrl:           wsUrl,
		persistSupport:  true,
		logger:          logger,
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
				sub, err = c.reconnect(headers, sub)
				if err != nil || sub == nil {
					c.logger.Error(err.Error())
				} else {
					break
				}
			}
		case header := <-headers:
			for _, sub := range c.subscriptions {
				startNum := new(big.Int).Add(sub.Height, big.NewInt(1))
				endNum := header.Number
				for startNum.Cmp(endNum) != 1 {
					nextBlock := new(big.Int).Add(startNum, big.NewInt(LostLogsQueryRange))
					if nextBlock.Cmp(endNum) == 1 {
						nextBlock = endNum
					}
					if startNum.Cmp(nextBlock) <= 0 {
						logs, err := c.rpcClient.FilterLogs(c.ctx, ethereum.FilterQuery{
							FromBlock: startNum,
							ToBlock:   nextBlock,
							Addresses: sub.Addresses,
							Topics:    sub.Topics,
						})
						if err != nil {
							c.logger.Error(err.Error())
							break
						}
						if len(logs) == 0 {
							if c.persistSupport && sub.IsPersisted {
								subModel, err := subTypes.ToSubscriptionModel(sub)
								if err != nil {
									c.logger.Error(err.Error())
									panic(err)
								}
								subModel.Height = nextBlock.String()
								err = c.subscriptionDao.UpdateHeight(c.ctx, subModel)
								if err != nil {
									c.logger.Error(err.Error())
									panic(err)
								}
							}
							sub.Height = nextBlock
						} else {
							blockRangeSub := &subTypes.BlockRangeLogs{
								ChainId: sub.ChainId,
								Logs:    logs,
								Start:   startNum,
								End:     nextBlock,
							}
							err = sub.Handler(subTypes.NewSubClientCtx(c.ctx), blockRangeSub)
							if err != nil {
								c.logger.Error(err.Error())
								break
							}
							if c.persistSupport && sub.IsPersisted {
								subModel, err := subTypes.ToSubscriptionModel(sub)
								if err != nil {
									c.logger.Error(err.Error())
									panic(err)
								}
								subModel.Height = nextBlock.String()
								err = c.subscriptionDao.UpdateHeight(c.ctx, subModel)
								if err != nil {
									c.logger.Error(err.Error())
									panic(err)
								}
							}
							sub.Height = nextBlock

						}
					}
					startNum = new(big.Int).Add(nextBlock, big.NewInt(1))
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
	if !c.persistSupport && isPersisted {
		return errors.New("client not support persist")
	}
	if _, ok := c.subscriptions[tag]; ok {
		return nil
	}

	if !isPersisted {
		currentHeigeht, err := c.rpcClient.BlockNumber(c.ctx)
		if err != nil {
			return err
		}
		sub := &subTypes.Subscription{
			ChainId:     int(c.chainId),
			Tag:         tag,
			Start:       big.NewInt(int64(currentHeigeht)),
			Height:      big.NewInt(int64(currentHeigeht)),
			Addresses:   addresses,
			Topics:      topics,
			IsPersisted: isPersisted,
			Handler:     handler,
		}
		c.subscriptions[tag] = sub
		return nil
	}

	subTag, err := c.subscriptionDao.QueryByChainIdAndTag(c.ctx, int(c.chainId), tag)
	if err == nil && subTag != nil && !isPersisted {
		return errors.New("persisted subscription already exists")
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
		c.subscriptions[tag] = subFromDb
	} else {
		currentHeigeht, err := c.rpcClient.BlockNumber(c.ctx)
		if err != nil {
			return err
		}
		sub := &subTypes.Subscription{
			ChainId:     int(c.chainId),
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
		subId, err := c.subscriptionDao.InsertSubscription(c.ctx, subModel)
		sub.Id = subId
		c.subscriptions[tag] = sub
	}
	return nil
}

func (c *SubClient) AddSubscriptionFromHeight(tag string, addresses []common.Address, topics [][]common.Hash, isPersisted bool, height *big.Int, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error {
	if !c.persistSupport && isPersisted {
		return errors.New("client not support persist")
	}
	if _, ok := c.subscriptions[tag]; ok {
		return nil
	}

	if !isPersisted {
		sub := &subTypes.Subscription{
			ChainId:     int(c.chainId),
			Tag:         tag,
			Start:       height,
			Height:      height,
			Addresses:   addresses,
			Topics:      topics,
			IsPersisted: isPersisted,
			Handler:     handler,
		}
		c.subscriptions[tag] = sub
		return nil
	}

	subTag, err := c.subscriptionDao.QueryByChainIdAndTag(c.ctx, int(c.chainId), tag)
	if err == nil && subTag != nil && !isPersisted {
		return errors.New("persisted subscription already exists")
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
		c.subscriptions[tag] = subFromDb
	} else {
		sub := &subTypes.Subscription{
			ChainId:     int(c.chainId),
			Tag:         tag,
			Start:       height,
			Height:      height,
			Addresses:   addresses,
			Topics:      topics,
			IsPersisted: isPersisted,
			Handler:     handler,
		}
		subModel, err := subTypes.ToSubscriptionModel(sub)
		if err != nil {
			return err
		}
		subId, err := c.subscriptionDao.InsertSubscription(c.ctx, subModel)
		sub.Id = subId
		c.subscriptions[tag] = sub
	}
	return nil
}

func (c *SubClient) RemoveSubsciption(tag string) error {
	if c.persistSupport {
		err := c.subscriptionDao.DeleteSubscription(c.ctx, tag, int(c.chainId))
		if err != nil {
			return err
		}
	}
	c.subscriptions[tag] = nil
	return nil
}

func (c *SubClient) GetSubscriptionStartAndHeight(tag string) (*big.Int, *big.Int, error) {
	for _, sub := range c.subscriptions {
		if sub.Tag == tag {
			return sub.Start, sub.Height, nil
		}
	}
	return nil, nil, errors.New("subscription not found")
}

func (c *SubClient) GetSubscriptionTags() []string {
	var tags []string
	for _, sub := range c.subscriptions {
		tags = append(tags, sub.Tag)
	}
	return tags
}

func (c *SubClient) GetLogsHistory(addresses []common.Address, topics [][]common.Hash, start *big.Int, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) error {
	end, err := c.rpcClient.BlockNumber(c.ctx)
	if err != nil {
		return err
	}
	if start.Cmp(big.NewInt(int64(end))) == 1 {
		return fmt.Errorf("start is greater than end")
	}
	go c.getLogsHis(addresses, topics, start, big.NewInt(int64(end)), handler)
	return nil
}

func (c *SubClient) getLogsHis(addresses []common.Address, topics [][]common.Hash, start *big.Int, end *big.Int, handler func(ctx *subTypes.SubClientCtx, brl *subTypes.BlockRangeLogs) (err error)) {
	heartbeatInterval := 5 * time.Second
	heartbeatTimer := time.NewTicker(heartbeatInterval)
	for start.Cmp(end) != 1 {
		nextBlock := new(big.Int).Add(start, big.NewInt(1999))
		if nextBlock.Cmp(end) == 1 {
			nextBlock = end
		}
		var logs []types.Log
		for range heartbeatTimer.C {
			var err error
			logs, err = c.rpcClient.FilterLogs(context.Background(), ethereum.FilterQuery{
				FromBlock: start,
				ToBlock:   nextBlock,
				Addresses: addresses,
				Topics:    topics,
			})
			if err == nil {
				break
			}
		}
		blockRangeSub := &subTypes.BlockRangeLogs{
			ChainId: int(c.GetChainId()),
			Logs:    logs,
			Start:   start,
			End:     nextBlock,
		}
		err := handler(subTypes.NewSubClientCtx(c.ctx), blockRangeSub)
		if err != nil {
			break
		}
		start = new(big.Int).Add(nextBlock, big.NewInt(1))
	}
}

func (c *SubClient) GetChainId() int64 {
	return c.chainId
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
