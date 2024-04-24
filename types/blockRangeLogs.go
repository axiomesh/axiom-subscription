package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockRangeLogs struct {
	ChainId int
	Start   *big.Int
	End     *big.Int
	Logs    []types.Log
}

func (brl *BlockRangeLogs) GetLogs() []types.Log {
	return brl.Logs
}

func (brl *BlockRangeLogs) GetChainId() int {
	return brl.ChainId
}

func (brl *BlockRangeLogs) GetStart() *big.Int {
	return brl.Start
}

func (brl *BlockRangeLogs) GetEnd() *big.Int {
	return brl.End
}
