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

var persistFn = Persist

type Persister struct {
	qA, qB, qC []Spool
	mu         sync.Mutex
	doneCh     chan struct{}
	ks         *keyspace.KeySpace
	timeChan   <-chan time.Time
}

type PersisterOpts struct {
	KeySpace *keyspace.KeySpace
	TimeChan <-chan time.Time
}

// NewPersister returns an empty, initialized Persister.
func NewPersister(opts PersisterOpts) *Persister {
	return &Persister{
		doneCh:   make(chan struct{}),
		ks:       opts.KeySpace,
		timeChan: opts.TimeChan,
	}
}

func (p *Persister) SpoolComplete(spool Spool, primary bool) {
	if spool.ContentLength() == 0 || spool.BackingStore == "" {
		// Cannot persist this Spool.
	} else if primary {
		// Attempt to immediately persist the Spool.
		go func() {
			if err := persistFn(context.Background(), spool); err != nil {
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
	var ticker *time.Ticker
	// If a timeChan has not been provided use default 1 min ticker.
	if p.timeChan == nil {
		ticker = time.NewTicker(time.Minute)
		p.timeChan = ticker.C
	}
	for done, exiting := false, false; !done; {
		if !exiting {
			select {
			case <-p.timeChan:
			case <-p.doneCh:
				exiting = true
				if ticker != nil {
					ticker.Stop()
				}
			}
		}

		for _, spool := range p.qA {
			// Prior to attempting to store the fragment confirm that fragment metadata
			// is the most up to date.
			if item, ok := allocator.LookupItem(p.ks, spool.Journal.String()); ok {
				var spec = item.ItemValue.(*pb.JournalSpec)
				spool.Fragment.BackingStore = spec.Fragment.Stores[0]
			}
			if err := persistFn(context.Background(), spool); err != nil {
				log.WithField("err", err).Warn("failed to persist Spool")
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
