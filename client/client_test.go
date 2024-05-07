package client

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/axiomesh/axiom-subscription/types"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var TestBin = "608080604052346013576095908160188239f35b5f80fdfe60808060405260043610156011575f80fd5b5f3560e01c634f9d719e146023575f80fd5b34605b575f366003190112605b5760207f70b02290e9eb0ce36e6019903dfe216e38dd640f02554473c477691b45a4df4c91438152a1005b5f80fdfea2646970667358221220f595928f6d4d40dbdec5e9b7afa78e0b53ca4a35405c19657e49dcd96329ca4064736f6c63430008190033"
var TestAbi = "[ { \"anonymous\": false, \"inputs\": [ { \"indexed\": false, \"internalType\": \"uint256\", \"name\": \"\", \"type\": \"uint256\" } ], \"name\": \"BlockNum\", \"type\": \"event\" }, { \"inputs\": [], \"name\": \"testEvent\", \"outputs\": [], \"stateMutability\": \"nonpayable\", \"type\": \"function\" } ]"
var privateKey = ""
var BlockNumEvent = "BlockNum(uint256)"

var BlockNumHash = common.BytesToHash(crypto.Keccak256([]byte(BlockNumEvent)))

func TestClient(t *testing.T) {
	rpcUrl := "http://127.0.0.1:8881"
	wsUrl := "ws://127.0.0.1:9991"

	db, err := sqlx.Connect("postgres", "user=postgres password=123456 host=localhost port=5432 database=bridge sslmode=disable")
	assert.Nil(t, err)
	log := slog.Logger{}

	chainClient, err := ethclient.Dial(rpcUrl)
	assert.Nil(t, err)
	privateKey, err := crypto.HexToECDSA(privateKey)
	assert.Nil(t, err)
	auth := bind.NewKeyedTransactor(privateKey)
	nonce, err := chainClient.NonceAt(context.Background(), auth.From, nil)
	assert.Nil(t, err)

	// 设置交易的 gas 限制
	auth.Nonce = big.NewInt(int64(nonce))
	auth.GasLimit = uint64(300000)

	// 设置交易的 gas 价格
	auth.GasPrice = big.NewInt(10000000000)
	abi, err := ParseABI(TestAbi)
	assert.Nil(t, err)

	address, _, _, err := bind.DeployContract(auth, abi, common.FromHex(TestBin), chainClient)
	assert.Nil(t, err)
	fmt.Println(address)
	time.Sleep(3 * time.Second)

	client, err := NewClient(rpcUrl, wsUrl, db, log)
	assert.Nil(t, err)
	err = client.AddSubscription("BlockNum", []common.Address{address}, [][]common.Hash{{BlockNumHash}}, true, BlockNumHandler)
	assert.Nil(t, err)

	err = client.AddSubscription("BlockNum2", []common.Address{address}, [][]common.Hash{{BlockNumHash}}, false, BlockNumHandler)
	assert.Nil(t, err)

	data, err := abi.Pack("testEvent")
	assert.Nil(t, err)

	newNonce := nonce + 1

	tx := ethtypes.NewTransaction(newNonce, address, big.NewInt(0), 1000000, big.NewInt(1000000000), data)
	signedTx, err := ethtypes.SignTx(tx, ethtypes.NewCancunSigner(big.NewInt(1356)), privateKey)
	assert.Nil(t, err)
	err = chainClient.SendTransaction(context.Background(), signedTx)
	assert.Nil(t, err)
	time.Sleep(3 * time.Second)
	client.RemoveSubsciption("BlockNum")
}

func TestCacheClient(t *testing.T) {
	rpcUrl := "http://127.0.0.1:8881"
	wsUrl := "ws://127.0.0.1:9991"
	log := slog.Logger{}

	chainClient, err := ethclient.Dial(rpcUrl)
	assert.Nil(t, err)
	privateKey, err := crypto.HexToECDSA(privateKey)
	assert.Nil(t, err)
	auth := bind.NewKeyedTransactor(privateKey)
	nonce, err := chainClient.NonceAt(context.Background(), auth.From, nil)
	assert.Nil(t, err)

	// 设置交易的 gas 限制
	auth.Nonce = big.NewInt(int64(nonce))
	auth.GasLimit = uint64(300000)

	// 设置交易的 gas 价格
	auth.GasPrice = big.NewInt(10000000000)
	abi, err := ParseABI(TestAbi)
	assert.Nil(t, err)

	address, _, _, err := bind.DeployContract(auth, abi, common.FromHex(TestBin), chainClient)
	assert.Nil(t, err)
	fmt.Println(address)
	time.Sleep(3 * time.Second)

	client, err := NewCacheClient(rpcUrl, wsUrl, log)
	assert.Nil(t, err)
	err = client.AddSubscription("BlockNum", []common.Address{address}, [][]common.Hash{{BlockNumHash}}, true, BlockNumHandler)
	assert.NotNil(t, err)

	err = client.AddSubscription("BlockNum", []common.Address{address}, [][]common.Hash{{BlockNumHash}}, false, BlockNumHandler)
	assert.Nil(t, err)

	data, err := abi.Pack("testEvent")
	assert.Nil(t, err)

	newNonce := nonce + 1

	tx := ethtypes.NewTransaction(newNonce, address, big.NewInt(0), 1000000, big.NewInt(1000000000), data)
	signedTx, err := ethtypes.SignTx(tx, ethtypes.NewCancunSigner(big.NewInt(1356)), privateKey)
	assert.Nil(t, err)
	err = chainClient.SendTransaction(context.Background(), signedTx)
	assert.Nil(t, err)
	time.Sleep(3 * time.Second)
	client.RemoveSubsciption("BlockNum")
}

func BlockNumHandler(ctx *types.SubClientCtx, brl *types.BlockRangeLogs) (err error) {
	fmt.Println("BlockNumHandler")
	fmt.Println(brl.Start)
	fmt.Println(brl.End)
	for _, log := range brl.Logs {
		fmt.Println(log.Data)
	}
	return nil
}

func ParseABI(abiStr string) (abi.ABI, error) {
	// 使用 abi.JSON 函数解析 ABI 字符串
	parsedABI, err := abi.JSON(strings.NewReader(abiStr))
	if err != nil {
		return abi.ABI{}, err
	}
	return parsedABI, nil
}
