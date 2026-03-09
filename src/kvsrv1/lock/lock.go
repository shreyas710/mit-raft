package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	ck       kvtest.IKVClerk
	lockname string
	id       string
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, id: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			// Lock key doesn't exist yet; create it in locked state
			if e := lk.ck.Put(lk.lockname, lk.id, 0); e == rpc.OK {
				return
			}
		} else if err == rpc.OK && val == "" {
			// Lock exists and is released; try to acquire
			if e := lk.ck.Put(lk.lockname, lk.id, ver); e == rpc.OK {
				return
			}
		} else {
			// Lock is held by someone else; back off and retry
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	for {
		_, ver, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			continue
		}
		if e := lk.ck.Put(lk.lockname, "", ver); e == rpc.OK || e == rpc.ErrMaybe {
			return
		}
	}
}
