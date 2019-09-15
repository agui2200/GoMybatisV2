package sessions

import (
	"context"
	"sync"
)

type SessionFactory struct {
	Engine     SessionEngine
	SessionMap sync.Map //map[string]Session
}

func (it SessionFactory) New(Engine SessionEngine) SessionFactory {
	it.Engine = Engine
	return it
}

// 加上上下文版
func (it *SessionFactory) NewSessionContext(ctx context.Context, mapperName string, sessionType SessionType) Session {
	return it.newSession(ctx, mapperName, sessionType)
}

func (it *SessionFactory) NewSession(mapperName string, sessionType SessionType) Session {
	return it.newSession(nil, mapperName, sessionType)
}

func (it *SessionFactory) newSession(ctx context.Context, mapperName string, sessionType SessionType) Session {
	if it.Engine == nil {
		panic("[GoMybatis] SessionFactory not init! you must call method SessionFactory.New(*)")
	}
	var newSession Session
	var err error
	switch sessionType {
	case SessionType_Default:
		var session, err = it.Engine.NewSession(mapperName)
		if err != nil {
			panic(err)
		}
		var factorySession = SessionFactorySession{
			Session: session,
			Factory: it,
		}
		newSession = &factorySession
		break
	case SessionType_Local:
		newSession, err = it.Engine.NewSession(mapperName)
		if err != nil {
			panic(err)
		}
		break
	default:
		panic("[GoMybatis] newSession() must have a SessionType!")
	}
	if ctx == nil {
		newSession.WithContext(context.TODO())
	} else {
		newSession.WithContext(ctx)
	}
	it.SessionMap.Store(newSession.Id(), newSession)
	return newSession
}

func (it *SessionFactory) GetSession(id string) Session {
	var v, _ = it.SessionMap.Load(id)
	return v.(Session)
}

func (it *SessionFactory) SetSession(id string, session Session) {
	it.SessionMap.Store(id, session)
}

func (it *SessionFactory) Close(id string) {
	var s, _ = it.SessionMap.Load(id)
	if s != nil {
		s.(Session).Close()
		it.SessionMap.Delete(id)
	}
}

func (it *SessionFactory) CloseAll(id string) {
	it.SessionMap.Range(func(key, value interface{}) bool {
		if value != nil {
			value.(Session).Close()
			it.SessionMap.Delete(key)
		}
		return true
	})
}
