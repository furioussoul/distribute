package raft

import "sync"

type Quorum struct {
	lock      sync.Mutex
	quorum    int
	succeeded int
	failed    int
	callback  func(elected bool)
	complete  bool
}

func makeQuorum(quorum int, callback func(elected bool)) *Quorum {
	qr := &Quorum{}
	qr.quorum = quorum
	qr.callback = callback
	qr.succeeded = 1
	return qr
}

func (qr *Quorum) checkComplete() {
	if !qr.complete && qr.callback != nil {
		if qr.succeeded >= qr.quorum {
			qr.complete = true
			qr.callback(true)
		} else if qr.failed >= qr.quorum {
			qr.complete = true
			qr.callback(false)
		}
	}
}

func (qr *Quorum) succeed() {
	qr.lock.Lock()
	defer qr.lock.Unlock()
	qr.succeeded++
	qr.checkComplete()
}

func (qr *Quorum) fail() {
	qr.lock.Lock()
	defer qr.lock.Unlock()
	qr.failed++
	qr.checkComplete()
}

func (qr *Quorum) cancel() {
	qr.lock.Lock()
	defer qr.lock.Unlock()
	qr.callback = nil
	qr.complete = true
}
