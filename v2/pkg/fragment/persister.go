package fragment

import (
	"context"
	"sync"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

const (
	persistInterval = time.Minute
)

type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
	ks         *keyspace.KeySpace
	ticker     *time.Ticker
	persistFn  func(ctx context.Context, spool Spool) error
}

// NewPersister returns an empty, initialized Persister.
func NewPersister(ks *keyspace.KeySpace) *Persister {
	return &Persister{
		doneCh:    make(chan struct{}),
		ks:        ks,
		persistFn: Persist,
		ticker:    time.NewTicker(persistInterval),
	}
}

func (p *Persister) SpoolComplete(spool Spool, primary bool) {
	if spool.ContentLength() == 0 {
		// Cannot persist this Spool.
	} else if primary {
		// Attempt to immediately persist the Spool.
		go func() {
			if err := p.persist(spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}()
	} else {
		p.queue(spool)
	}
	return
}

func (p *Persister) Finish() {
	p.doneCh <- struct{}{}
	<-p.doneCh
}

func (p *Persister) queue(spool Spool) {
	defer p.mu.Unlock()
	p.mu.Lock()

	p.qC = append(p.qC, spool)
}

func (p *Persister) Serve() {
	for done, exiting := false, false; !done; {
		if !exiting {
			select {
			case <-p.ticker.C:
			case <-p.doneCh:
				exiting = true
				p.ticker.Stop()
			}
		}

		for _, spool := range p.qA {
			if err := p.persist(spool); err != nil {
				log.WithFields(log.Fields{
					"journal": spool.Journal,
					"err":     err,
				}).Warn("failed to persist Spool")
				p.queue(spool)
			}
		}

		// Rotate queues.
		p.mu.Lock()
		p.qA, p.qB, p.qC = p.qB, p.qC, p.qA[:0]

		if exiting && len(p.qA) == 0 && len(p.qB) == 0 {
			done = true
		}
		p.mu.Unlock()
	}
	close(p.doneCh)
}

func (p *Persister) persist(spool Spool) error {
	// Prior to attempting to store the fragment confirm that fragment metadata
	// is the most up to date.
	if item, ok := allocator.LookupItem(p.ks, spool.Journal.String()); ok {
		var spec = item.ItemValue.(*pb.JournalSpec)
		// The spec has been updated and to no longer persist fragments.
		// Drop this fragment.
		if len(spec.Fragment.Stores) == 0 {
			return nil
		}
		spool.BackingStore = spec.Fragment.Stores[0]
	} else {
		log.WithField("journal", spool.Journal).Warn("journal spec has been removed")
		return nil
	}

	return p.persistFn(context.Background(), spool)
}
