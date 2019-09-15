package GoMybatis

import "context"

type SessionSupport struct {
	NewSession func(ctx context.Context) (Session, error) //session为事务操作
}
