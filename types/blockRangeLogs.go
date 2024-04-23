package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockRangeLogs struct {
	ChainID int
	Start   *big.Int
	End     *big.Int
	Logs    []types.Log
}

func (brl *BlockRangeLogs) GetLogs() []types.Log {
	return brl.Logs
}
