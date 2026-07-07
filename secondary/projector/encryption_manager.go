// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package projector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// LogdataKDT is the only KDT this manager handles.
var LogdataKDT = c.KeyDataType{TypeName: "log"}

// CipherNameNone mirrors cbauth's "encryption disabled" cipher name.
const CipherNameNone = "NONE"

// ErrEncrMgrNotReady is returned by getInUseKeysCallback before MarkReady is called.
var ErrEncrMgrNotReady = errors.New("EncryptionMgr not ready to process requests")

// errDropActiveKey is returned when a drop request includes the currently active key.
var errDropActiveKey = errors.New("DropKey received for active key")

// errNoDropKeyHook is returned when a drop is requested but no hook is registered.
var errNoDropKeyHook = errors.New("no DropKey hook registered")

// workChCapacity is the buffer size of the worker channel.
const workChCapacity = 1_000

// StatsHooks is the contract between EncryptionMgr and statsManager.
// statsManager registers these via SetStatsHooks after it has constructed
// its logstats FileHandler.
type StatsHooks struct {
	// UpdateActiveKey is invoked when cbauth signals a new active key for "log".
	UpdateActiveKey func(earkey c.EaRKey) error

	// DropKey is invoked when cbauth requests that keys be retired.
	DropKey func(activeEarKey c.EaRKey, dropKeyIDs []string) error
}

type workItemType int

const (
	workItemUpdate workItemType = iota
	workItemDrop
)

type workItem struct {
	typ        workItemType
	activeKey  c.EaRKey
	dropKeyIDs []string
}

// EncryptionMgr owns the cbauth callback wiring for the "log" KDT and exposes
// hooks that the statsManager plugs into.
type EncryptionMgr struct {
	config c.Config
	cache  *c.EaRKeyCache

	// In-use key IDs reported by the file handler / stats persister.
	inUseKeys map[string]struct{}
	muid      sync.Mutex

	// Stats manager hooks; set after construction.
	hooks   StatsHooks
	hooksMu sync.RWMutex

	// Worker serialization.
	// Updates are sent directly to workCh (buffered, idempotent).
	// Drops must not be lost, so they are tracked via inFlight+pendingDrops:
	// cbauth serialises drops (no new drop until KeysDropComplete), so
	// pendingDrops rarely holds more than one entry.
	cbMu         sync.Mutex
	workCh       chan workItem
	inFlight     *workItem  // non-nil while a drop is being processed
	pendingDrops []workItem // drops waiting behind an in-flight drop

	isReady   atomic.Bool
	stopCh    chan struct{}
	stopOnce  sync.Once
	seedCh    chan struct{}
	seedOnce  sync.Once
	hooksCh   chan struct{}
	hooksOnce sync.Once
}

func (e *EncryptionMgr) initialSeed() {
	e.seedOnce.Do(func() {
		defer close(e.seedCh)
		if err := e.cache.Refresh(context.Background(), LogdataKDT); err != nil {
			logging.Errorf("EncryptionMgr: initial seed: %v", err)
		}
		logging.Infof("EncryptionMgr:initialSeed done with initial seed")
	})
}

// NewEncryptionMgr constructs the manager, seeds the key cache for the "log"
// KDT via cbauth, starts the worker goroutine, and registers cbauth callbacks.
func NewEncryptionMgr(config c.Config) (*EncryptionMgr, error) {
	e := &EncryptionMgr{
		config:    config,
		cache:     c.NewEaRKeyCache(&c.CbauthEaRKeyProvider{}),
		inUseKeys: make(map[string]struct{}),
		workCh:    make(chan workItem, workChCapacity),
		stopCh:    make(chan struct{}),
		seedCh:    make(chan struct{}),
		hooksCh:   make(chan struct{}),
	}

	go e.initialSeed()

	go e.run()

	if err := cbauth.RegisterEncryptionKeysCallbacks(
		e.refreshKeysCallback,
		e.getInUseKeysCallback,
		e.dropKeysCallback,
		e.synchronizeKeyFilesCallback,
	); err != nil {
		close(e.stopCh)
		return nil, fmt.Errorf("EncryptionMgr: RegisterEncryptionKeysCallbacks: %w", err)
	}

	if c.GetBuildMode() == c.ENTERPRISE {
		<-e.seedCh
	}

	logging.Infof("EncryptionMgr: initialized for kdt=%v", LogdataKDT)
	return e, nil
}

// SetStatsHooks registers the statsManager callbacks.
func (e *EncryptionMgr) SetStatsHooks(h StatsHooks) {
	e.hooksMu.Lock()
	e.hooks = h
	e.hooksMu.Unlock()
	e.hooksOnce.Do(func() { close(e.hooksCh) })
}

// MarkReady allows getInUseKeysCallback to start returning real data.
// Call from statsManager after startup-time bookkeeping is done.
func (e *EncryptionMgr) MarkReady() { e.isReady.Store(true) }

// Close stops the worker goroutine.
func (e *EncryptionMgr) Close() {
	e.stopOnce.Do(func() {
		select {
		case <-e.stopCh:
		default:
			close(e.stopCh)
		}
	})
}

// GetKeyCipherById returns the key bytes and cipher name for keyID.
// Empty keyID returns ([]byte{}, CipherNameNone) — plaintext sentinel.
func (e *EncryptionMgr) GetKeyCipherById(keyID string) ([]byte, string) {
	if keyID == "" {
		return []byte{}, CipherNameNone
	}
	k, err := e.cache.GetKeyByIDOrFetch(context.Background(), LogdataKDT, keyID)
	if err != nil {
		logging.Warnf("EncryptionMgr:GetKeyCipherById key:%v err:%v", keyID, err)
		return []byte{}, CipherNameNone
	}
	return k.Key, k.Cipher
}

// GetActiveKeyIdCipher returns (key, id, cipher) for the currently active key,
// or ([]byte{}, "", CipherNameNone) if encryption is disabled or an error occurs.
func (e *EncryptionMgr) GetActiveKeyIdCipher() ([]byte, string, string) {
	k, err := e.cache.GetActiveKeyOrFetch(context.Background(), LogdataKDT)
	if err != nil {
		logging.Warnf("EncryptionMgr:GetActiveKeyIdCipher err:%v", err)
		return []byte{}, "", CipherNameNone
	}
	if k.Id == "" {
		return []byte{}, "", CipherNameNone
	}
	return k.Key, k.Id, k.Cipher
}

// SetInUseKeys records that keyID is currently used by a stats log file.
func (e *EncryptionMgr) SetInUseKeys(keyID string) {
	e.muid.Lock()
	e.inUseKeys[keyID] = struct{}{}
	e.muid.Unlock()
}

// DropInUseKeys removes keyIDs after a successful drop.
func (e *EncryptionMgr) DropInUseKeys(keyIDs []string) {
	e.muid.Lock()
	for _, id := range keyIDs {
		delete(e.inUseKeys, id)
	}
	e.muid.Unlock()
}

// --- cbauth callbacks ---

func (e *EncryptionMgr) getInUseKeysCallback(kdt c.KeyDataType) ([]string, error) {
	if kdt.TypeName != LogdataKDT.TypeName {
		return nil, nil
	}
	if !e.isReady.Load() {
		return nil, ErrEncrMgrNotReady
	}
	e.muid.Lock()
	defer e.muid.Unlock()
	out := make([]string, 0, len(e.inUseKeys))
	for k := range e.inUseKeys {
		out = append(out, k)
	}
	return out, nil
}

func (e *EncryptionMgr) refreshKeysCallback(kdt c.KeyDataType) error {
	if kdt.TypeName != LogdataKDT.TypeName {
		return nil
	}
	if err := e.cache.Refresh(context.Background(), kdt); err != nil {
		wrapped := fmt.Errorf("EncryptionMgr:refreshKeysCallback: %w", err)
		logging.Warnf("%v", wrapped)
		return wrapped
	}
	active, _ := e.cache.GetActiveKey(kdt)
	e.enqueueUpdate(workItem{typ: workItemUpdate, activeKey: active})
	return nil
}

func (e *EncryptionMgr) dropKeysCallback(kdt c.KeyDataType, keyIDs []string) {
	if kdt.TypeName != LogdataKDT.TypeName {
		return
	}
	if err := e.cache.Refresh(context.Background(), kdt); err != nil {
		logging.Warnf("EncryptionMgr:dropKeysCallback refresh err:%v", err)
		if err2 := cbauth.KeysDropComplete(kdt, err); err2 != nil {
			logging.Warnf("EncryptionMgr:dropKeysCallback KeysDropComplete err:%v", err2)
		}
		return
	}
	active, _ := e.cache.GetActiveKey(kdt)

	// Active key must not be in the drop list.
	for _, id := range keyIDs {
		if id == active.Id && id != "" {
			dropErr := fmt.Errorf("%w: keyid=%v", errDropActiveKey, id)
			logging.Warnf("EncryptionMgr:dropKeysCallback %v", dropErr)
			if err2 := cbauth.KeysDropComplete(kdt, dropErr); err2 != nil {
				logging.Warnf("EncryptionMgr:dropKeysCallback KeysDropComplete err:%v", err2)
			}
			return
		}
	}

	e.enqueueDrop(workItem{typ: workItemDrop, activeKey: active, dropKeyIDs: keyIDs})
}

func (e *EncryptionMgr) synchronizeKeyFilesCallback(kdt c.KeyDataType) error { return nil }

// --- enqueue + worker ---

// enqueueUpdate sends a key-rotation update directly to workCh.
func (e *EncryptionMgr) enqueueUpdate(w workItem) {
	e.workCh <- w
}

// enqueueDrop appends to pendingDrops if a drop/update is already in flight.
func (e *EncryptionMgr) enqueueDrop(w workItem) {
	e.cbMu.Lock()
	if e.inFlight != nil {
		e.pendingDrops = append(e.pendingDrops, w)
		e.cbMu.Unlock()
		return
	}
	e.inFlight = &w
	e.cbMu.Unlock()
	e.workCh <- w
}

func (e *EncryptionMgr) run() {
	<-e.seedCh
	select {
	case <-e.hooksCh:
	case <-e.stopCh:
		logging.Infof("EncryptionMgr:run exiting")
		return
	}
	logging.Infof("EncryptionMgr:run started")
	for {
		select {
		case <-e.stopCh:
			logging.Infof("EncryptionMgr:run exiting")
			return
		case w := <-e.workCh:
			e.handle(w)
			e.advance()
		}
	}
}

func (e *EncryptionMgr) handle(w workItem) {
	e.hooksMu.RLock()
	hook := e.hooks
	e.hooksMu.RUnlock()

	switch w.typ {
	case workItemUpdate:
		if hook.UpdateActiveKey == nil {
			logging.Warnf("EncryptionMgr:handle update skipped — no hook registered (keyId=%v)", w.activeKey.Id)
			return
		}
		if err := hook.UpdateActiveKey(w.activeKey); err != nil {
			logging.Warnf("EncryptionMgr:handle UpdateActiveKey err:%v key:%v", err, w.activeKey.Id)
		}

	case workItemDrop:
		var err error
		if hook.DropKey == nil {
			err = errNoDropKeyHook
			logging.Warnf("EncryptionMgr:handle drop skipped — %v", err)
		} else {
			err = hook.DropKey(w.activeKey, w.dropKeyIDs)
			if err == nil {
				e.DropInUseKeys(w.dropKeyIDs)
			} else {
				logging.Warnf("EncryptionMgr:handle DropKey err:%v drop=%v", err, w.dropKeyIDs)
			}
		}
		if err2 := cbauth.KeysDropComplete(LogdataKDT, err); err2 != nil {
			logging.Warnf("EncryptionMgr:handle KeysDropComplete err:%v", err2)
		}
	}
}

// advance clears inFlight and dispatches the next pending drop, if any.
func (e *EncryptionMgr) advance() {
	e.cbMu.Lock()
	e.inFlight = nil

	if len(e.pendingDrops) == 0 {
		e.cbMu.Unlock()
		return
	}
	next := e.pendingDrops[0]
	e.pendingDrops = e.pendingDrops[1:]
	e.inFlight = &next
	e.cbMu.Unlock()

	e.workCh <- next
}
