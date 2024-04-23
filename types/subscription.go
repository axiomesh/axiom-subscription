package types

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/axiomesh/axiom-subscription/internal/model"
	"github.com/ethereum/go-ethereum/common"
)

type handlerFunc func(*BlockRangeLogs) error

type Subscription struct {
	Id        int
	Tag       string
	ChainId   int
	Addresses []common.Address
	Topics    [][]common.Hash
	Start     *big.Int
	Height    *big.Int
	Handler   handlerFunc
}

func FromSubscriptionModel(subscription *model.Subscription) (*Subscription, error) {
	var addresses []common.Address
	err := json.Unmarshal([]byte(subscription.Addresses), &addresses)
	if err != nil {
		return nil, err
	}
	var topics [][]common.Hash
	err = json.Unmarshal([]byte(subscription.Topics), &topics)
	if err != nil {
		return nil, err
	}

	start, res := new(big.Int).SetString(subscription.Start, 10)
	if !res {
		return nil, fmt.Errorf("start is not a number")
	}
	height, res := new(big.Int).SetString(subscription.Height, 10)
	if !res {
		return nil, fmt.Errorf("start is not a number")
	}

	Subscription := &Subscription{
		Id:        subscription.ID,
		Tag:       subscription.Tag,
		ChainId:   subscription.ChainID,
		Addresses: addresses,
		Topics:    topics,
		Start:     start,
		Height:    height,
	}
	return Subscription, nil
}

func ToSubscriptionModel(subscription *Subscription) (*model.Subscription, error) {
	addressJson, err := json.Marshal(subscription.Addresses)
	if err != nil {
		return nil, err
	}
	topicsJson, err := json.Marshal(subscription.Topics)
	if err != nil {
		return nil, err
	}
	return &model.Subscription{
		ID:        subscription.Id,
		ChainID:   subscription.ChainId,
		Tag:       subscription.Tag,
		Addresses: string(addressJson),
		Topics:    string(topicsJson),
		Start:     subscription.Start.String(),
		Height:    subscription.Height.String(),
	}, nil
}
