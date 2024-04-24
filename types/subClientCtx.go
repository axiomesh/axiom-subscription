package types

import "context"

type SubClientCtx struct {
	Ctx context.Context
}

func NewSubClientCtx(ctx context.Context) *SubClientCtx {
	return &SubClientCtx{
		Ctx: ctx,
	}
}
