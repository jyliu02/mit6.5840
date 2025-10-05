package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const Debug = false

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	lockKey string
	ownerID string // Unique identifier for this lock instance
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// Generate unique owner ID for this lock instance
	ownerID := kvtest.RandValue(8)

	lk := &Lock {
		ck:      ck,
		lockKey: l,
		ownerID: ownerID,
	}
	
	DPrintf("Lock %s: created with owner %s", l, ownerID)
	return lk
}

func (lk *Lock) Acquire() {
	DPrintf("Lock %s: %s attempting to acquire", lk.lockKey, lk.ownerID)
	
	for {
		// First, try to create the lock with version 0 (brand new lock)
		err := lk.ck.Put(lk.lockKey, lk.ownerID, 0)
		if err == rpc.OK {
			// Successfully acquired the lock
			DPrintf("Lock %s: %s acquired (new lock)", lk.lockKey, lk.ownerID)
			return
		}
		if err == rpc.ErrMaybe {
			// Ambiguous case - we might have acquired the lock
			DPrintf("Lock %s: %s got ErrMaybe on new lock creation, checking ownership", lk.lockKey, lk.ownerID)
			if lk.checkOwnership() {
				DPrintf("Lock %s: %s confirmed ownership after ErrMaybe", lk.lockKey, lk.ownerID)
				return
			}
			// We don't own it, continue trying
		}
		
		// Lock exists, check if it's free
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			// Key disappeared, try again
			DPrintf("Lock %s: %s key disappeared, retrying", lk.lockKey, lk.ownerID)
			continue
		}
		if err != rpc.OK {
			// Network error, retry
			DPrintf("Lock %s: %s get error %v, retrying", lk.lockKey, lk.ownerID, err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		// If lock is marked as FREE, try to acquire it
		if value == "FREE" {
			DPrintf("Lock %s: %s found FREE lock, attempting acquire", lk.lockKey, lk.ownerID)
			err = lk.ck.Put(lk.lockKey, lk.ownerID, version)
			if err == rpc.OK {
				// Successfully acquired the lock
				DPrintf("Lock %s: %s acquired (from FREE)", lk.lockKey, lk.ownerID)
				return
			}
			if err == rpc.ErrMaybe {
				// Ambiguous case - we might have acquired the lock
				DPrintf("Lock %s: %s got ErrMaybe on FREE lock acquire, checking ownership", lk.lockKey, lk.ownerID)
				if lk.checkOwnership() {
					DPrintf("Lock %s: %s confirmed ownership after ErrMaybe", lk.lockKey, lk.ownerID)
					return
				}
				// We don't own it, continue trying
			}
			DPrintf("Lock %s: %s failed to acquire FREE lock: %v", lk.lockKey, lk.ownerID, err)
			// If ErrVersion, someone else got it first, retry
		} else {
			DPrintf("Lock %s: %s lock held by %s", lk.lockKey, lk.ownerID, value)
		}
		
		// Lock is held by someone else or we failed to acquire, wait and retry
		time.Sleep(10 * time.Millisecond)
	}
}

// checkOwnership verifies if we currently own the lock
func (lk *Lock) checkOwnership() bool {
	value, _, err := lk.ck.Get(lk.lockKey)
	return err == rpc.OK && value == lk.ownerID
}

func (lk *Lock) Release() {
	DPrintf("Lock %s: %s attempting to release", lk.lockKey, lk.ownerID)
	
	for {
		// Get current lock state
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			// Lock already released or never acquired
			DPrintf("Lock %s: %s lock not found (already released)", lk.lockKey, lk.ownerID)
			return
		}
		if err != rpc.OK {
			// Network error, retry
			DPrintf("Lock %s: %s get error during release: %v", lk.lockKey, lk.ownerID, err)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Check if we own the lock
		if value == lk.ownerID {
			// We own the lock, release it by marking as free
			DPrintf("Lock %s: %s releasing owned lock", lk.lockKey, lk.ownerID)
			err = lk.ck.Put(lk.lockKey, "FREE", version)
			if err == rpc.OK {
				DPrintf("Lock %s: %s successfully released", lk.lockKey, lk.ownerID)
				return
			}
			if err == rpc.ErrMaybe {
				// Ambiguous case - we might have released the lock
				DPrintf("Lock %s: %s got ErrMaybe on release, assuming success", lk.lockKey, lk.ownerID)
				return
			}
			DPrintf("Lock %s: %s release failed: %v, retrying", lk.lockKey, lk.ownerID, err)
			// If ErrVersion, someone else modified it, retry
		} else if value == "FREE" {
			// Lock is already free
			DPrintf("Lock %s: %s lock already FREE", lk.lockKey, lk.ownerID)
			return
		} else {
			// We don't own the lock
			DPrintf("Lock %s: %s cannot release, owned by %s", lk.lockKey, lk.ownerID, value)
			return
		}
		// If we got ErrVersion, retry
		time.Sleep(10 * time.Millisecond)
	}
}
