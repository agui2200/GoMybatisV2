package sessions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/agui2200/GoMybatis/logger"
	"github.com/agui2200/GoMybatis/sessions/tx"
	"github.com/agui2200/GoMybatis/sqlbuilder"
	"github.com/agui2200/GoMybatis/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/opentracing/opentracing-go"
	"runtime"
	"strconv"
)

const (
	driverMysql = "mysql"
)

type urlInfo struct {
	userName string
	instance string
	addr     string
}

//本地直连session
type LocalSession struct {
	SessionId      string
	driver         string
	url            string
	urlInfo        urlInfo
	db             *sql.DB
	stmt           *sql.Stmt
	txStack        tx.TxStack
	savePointStack *tx.SavePointStack
	isClosed       bool
	session        *LocalSession

	logSystem logger.Log
	ctx       context.Context
}

func (it LocalSession) New(driver string, url string, db *sql.DB, logSystem logger.Log) LocalSession {
	switch driver {
	case driverMysql:
		c, err := mysql.ParseDSN(url)
		if err != nil {
			panic(fmt.Sprintf("[GoMybatis] connect url:[%s] , [%s]", url, err.Error()))
		}
		it.urlInfo.addr = c.Addr
		it.urlInfo.userName = c.User
		it.urlInfo.instance = c.DBName
	}
	return LocalSession{
		SessionId: utils.CreateUUID(),
		db:        db,
		txStack:   tx.TxStack{}.New(),
		driver:    driver,
		url:       url,
		logSystem: logSystem,
		ctx:       it.ctx,
	}
}

func (it *LocalSession) Id() string {
	return it.SessionId
}

func (it *LocalSession) Begin() error {
	return it.BeginTrans(tx.PROPAGATION_REQUIRED)
}

func (it *LocalSession) BeginTrans(p tx.Propagation) (err error) {
	if it.logSystem != nil {
		it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] Begin session(Propagation:" + string(p) + ")"))
	}
	if it.isClosed == true {
		return utils.NewError("LocalSession", " can not Begin() a Closed Session!")
	}
	span, ctx := it.startSpanFromContext("begin")
	defer func() {
		if span != nil {
			it.ctx = ctx
			if err != nil {
				it.errorToSpan(span, err)
				span.Finish()
			}
		}
	}()
	switch p {
	case tx.PROPAGATION_REQUIRED: //end
		if it.txStack.Len() > 0 {
			t, p := it.txStack.Last()
			it.txStack.Push(ctx, t, p)
			return nil
		} else {
			var t, err = it.db.Begin()
			err = it.dbErrorPack(err)
			if err == nil {
				it.txStack.Push(ctx, t, &p)
			}
			return err
		}
	case tx.PROPAGATION_SUPPORTS: //end
		if it.txStack.Len() > 0 {
			return nil
		} else {
			//非事务
			return nil
		}
	case tx.PROPAGATION_MANDATORY: //end
		if it.txStack.Len() > 0 {
			return nil
		} else {
			return errors.New("[GoMybatis] PROPAGATION_MANDATORY Nested transaction exception! current not have a transaction!")
		}
	case tx.PROPAGATION_REQUIRES_NEW:
		if it.txStack.Len() > 0 {
			//TODO stop old tx
		}
		//TODO new session(tx)
		var db, e = sql.Open(it.driver, it.url)
		if e != nil {
			return e
		}
		var sess = LocalSession{}.New(it.driver, it.url, db, it.logSystem) //same PROPAGATION_REQUIRES_NEW
		sess.WithContext(it.ctx)
		it.session = &sess
		break
	case tx.PROPAGATION_NOT_SUPPORTED:
		if it.txStack.Len() > 0 {
			//TODO stop old tx
		}
		//TODO new session( no tx)
		var db, e = sql.Open(it.driver, it.url)
		if e != nil {
			return e
		}
		var sess = LocalSession{}.New(it.driver, it.url, db, it.logSystem)
		sess.WithContext(it.ctx)
		it.session = &sess
		break
	case tx.PROPAGATION_NEVER: //END
		if it.txStack.Len() > 0 {
			return errors.New("[GoMybatis] PROPAGATION_NEVER  Nested transaction exception! current Already have a transaction!")
		}
		break
	case tx.PROPAGATION_NESTED: //TODO REQUIRED 类似，增加 save point
		if it.savePointStack == nil {
			var savePointStack = tx.SavePointStack{}.New()
			it.savePointStack = &savePointStack
		}
		if it.txStack.Len() > 0 {
			t, p := it.txStack.Last()
			it.txStack.Push(ctx, t, p)
			return nil
		} else {
			var t, err = it.db.Begin()
			err = it.dbErrorPack(err)
			if err == nil {
				it.txStack.Push(ctx, t, &p)
			}
			return err
		}
	case tx.PROPAGATION_NOT_REQUIRED: //end
		if it.txStack.Len() > 0 {
			return errors.New("[GoMybatis] PROPAGATION_NOT_REQUIRED Nested transaction exception! current Already have a transaction!")
		} else {
			var t, err = it.db.Begin()
			err = it.dbErrorPack(err)
			if err == nil {
				it.txStack.Push(ctx, t, &p)
			}
			return err
		}
	default:
		panic("[GoMybatis] Nested transaction exception! not support PROPAGATION in begin!")
	}
	return nil
}

func (it *LocalSession) Commit() (err error) {
	if it.isClosed == true {
		return utils.NewError("LocalSession", " can not Commit() a Closed Session!")
	}

	if it.session != nil {
		var e = it.session.Commit()
		it.session.Close()
		it.session = nil
		if e != nil {
			return e
		}
	}
	var t *sql.Tx
	var p *tx.Propagation
	ctx, t, p := it.txStack.Pop()
	span, _ := opentracing.StartSpanFromContext(ctx, "commit")
	defer func() {
		if span != nil {
			if err != nil {
				it.errorToSpan(span, err)
			}
			span.Finish()
			if ctx != nil {
				opentracing.SpanFromContext(ctx).Finish()
			}
		}
	}()
	if t != nil && p != nil {
		if *p == tx.PROPAGATION_NESTED {
			if it.savePointStack == nil {
				var stack = tx.SavePointStack{}.New()
				it.savePointStack = &stack
			}
			var pId = "p" + strconv.Itoa(it.txStack.Len()+1)
			it.savePointStack.Push(pId)
			if it.logSystem != nil {
				it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] exec " + "savepoint " + pId))
			}
			_, e := t.Exec("savepoint " + pId)
			e = it.dbErrorPack(e)
			if e != nil {
				return e
			}
		}
		if it.txStack.Len() == 0 {
			if it.logSystem != nil {
				it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] Commit tx session:" + it.Id()))
			}
			var err = t.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (it *LocalSession) Rollback() (err error) {
	if it.isClosed == true {
		return utils.NewError("LocalSession", " can not Rollback() a Closed Session!")
	}

	if it.session != nil {
		var e = it.session.Rollback()
		it.session.Close()
		it.session = nil
		if e != nil {
			return e
		}
	}
	ctx, t, p := it.txStack.Pop()
	span, _ := opentracing.StartSpanFromContext(ctx, "rollback")
	defer func() {
		if span != nil {
			if err != nil {
				it.errorToSpan(span, err)
			}
			span.Finish()
			if ctx != nil {
				opentracing.SpanFromContext(ctx).Finish()
			}
		}
	}()
	if t != nil && p != nil {
		if *p == tx.PROPAGATION_NESTED {
			if it.savePointStack == nil {
				var stack = tx.SavePointStack{}.New()
				it.savePointStack = &stack
			}
			var point = it.savePointStack.Pop()
			if point != nil {
				if it.logSystem != nil {
					it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] exec ====================" + "rollback to " + *point))
				}
				_, e := t.Exec("rollback to " + *point)
				e = it.dbErrorPack(e)
				if e != nil {
					return e
				}
			}
		}

		if it.txStack.Len() == 0 {
			if it.logSystem != nil {
				it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] Rollback Session"))
			}
			var err = t.Rollback()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (it *LocalSession) LastPROPAGATION() *tx.Propagation {
	if it.txStack.Len() != 0 {
		var _, pr = it.txStack.Last()
		return pr
	}
	return nil
}

func (it *LocalSession) Close() {
	if it.logSystem != nil {
		it.logSystem.Println([]byte("[GoMybatis] [" + it.Id() + "] Close session"))
	}
	if it.session != nil {
		it.session.Close()
		it.session = nil
	}
	if it.db != nil {
		if it.stmt != nil {
			it.stmt.Close()
		}

		for i := 0; i < it.txStack.Len(); i++ {
			var ctx, tx, _ = it.txStack.Pop()
			if tx != nil {
				tx.Rollback()
			}
			if ctx != nil {
				opentracing.SpanFromContext(ctx).Finish()
			}
		}
		it.db = nil
		it.stmt = nil
		it.isClosed = true
	}
}

func (it *LocalSession) Query(sqlorArgs string) (res []map[string][]byte, err error) {
	if it.isClosed == true {
		return nil, utils.NewError("LocalSession", " can not Query() a Closed Session!")
	}
	if it.session != nil {
		return it.session.Query(sqlorArgs)
	}
	// 开启 span
	span, _ := it.startSpanFromContext("query")
	if span != nil {
		span.SetTag("db.statement", sqlorArgs)
		defer func() {
			if err != nil {
				it.errorToSpan(span, err)
			}
			span.Finish()
		}()
	}
	var rows *sql.Rows
	var t, _ = it.txStack.Last()
	if t != nil {

		rows, err = t.QueryContext(it.ctx, sqlorArgs)
		err = it.dbErrorPack(err)
	} else {
		rows, err = it.db.QueryContext(it.ctx, sqlorArgs)
		err = it.dbErrorPack(err)
	}
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return nil, err
	} else {
		return sqlbuilder.Rows2maps(rows)
	}
}

func (it *LocalSession) Exec(sqlorArgs string) (res *Result, err error) {
	if it.isClosed == true {
		return nil, utils.NewError("LocalSession", " can not Exec() a Closed Session!")
	}
	// 开启 span
	span, _ := it.startSpanFromContext("exec")
	if span != nil {
		span.SetTag("db.statement", sqlorArgs)
		defer func() {
			if err != nil {
				it.errorToSpan(span, err)
			}
			span.Finish()
		}()
	}

	if it.session != nil {
		return it.session.Exec(sqlorArgs)
	}

	var result sql.Result
	var t, _ = it.txStack.Last()
	if t != nil {
		result, err = t.ExecContext(it.ctx, sqlorArgs)
		err = it.dbErrorPack(err)
	} else {
		result, err = it.db.ExecContext(it.ctx, sqlorArgs)
		err = it.dbErrorPack(err)
	}
	if err != nil {
		return nil, err
	} else {
		var LastInsertId, _ = result.LastInsertId()
		var RowsAffected, _ = result.RowsAffected()
		return &Result{
			LastInsertId: LastInsertId,
			RowsAffected: RowsAffected,
		}, nil
	}
}

func (it *LocalSession) dbErrorPack(e error) error {
	if e != nil {
		var sqlError = errors.New("[GoMybatis][LocalSession]" + e.Error())
		return sqlError
	}
	return nil
}

func (it *LocalSession) WithContext(ctx context.Context) {
	if it.ctx == nil {
		it.ctx = ctx
	}
}

func (it *LocalSession) startSpanFromContext(opName string) (s opentracing.Span, c context.Context) {
	prefix := "goMybatis"
	s, c = opentracing.StartSpanFromContext(it.ctx, prefix+"."+opName)
	s.SetTag("db.instance", it.url)
	s.SetTag("db.type", "sql")
	s.SetTag("db.user", it.urlInfo.userName)
	s.SetTag("db.session", it.Id())

	s.SetTag("peer.address", it.urlInfo.addr)
	s.SetTag("span.kind", "client")
	//s.SetTag("db.statement", sql)
	return
}

func (it *LocalSession) errorToSpan(s opentracing.Span, err error) {
	s.SetTag("event", "error")
	// 加入一些runtime的内容，方便调试
	pc := make([]uintptr, 20) // at least 1 entry needed
	n := runtime.Callers(5, pc)
	frames := runtime.CallersFrames(pc[:n])
	s.SetTag("message", err)
	var stack []string
	for {
		frame, more := frames.Next()
		m := fmt.Sprintf("%s:%d %s \r\n", frame.File, frame.Line, frame.Function)
		stack = append(stack, m)
		if !more {
			break
		}
	}
	s.SetTag("error.object", stack)
}
