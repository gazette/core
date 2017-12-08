package gazette

import (
	"bytes"
	"net/url"
	"time"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/consensus"
	"github.com/LiveRamp/gazette/consensus/allocator"
	"github.com/LiveRamp/gazette/journal"
	"github.com/LiveRamp/gazette/metrics"
)

const ServiceRoot = "/gazette/cluster"

type Runner struct {
	client        etcd.Client
	localRouteKey string
	replicaCount  int
	router        *Router
}

func NewRunner(client etcd.Client, localRouteKey string, replicaCount int, router *Router) *Runner {
	var runner = Runner{
		client:        client,
		localRouteKey: localRouteKey,
		replicaCount:  replicaCount,
		router:        router,
	}

	return &runner
}

func (r *Runner) Run() error {
	return consensus.CreateAndAllocateWithSignalHandling(r)
}

// consumer.Allocator implementation.
func (r *Runner) FixedItems() []string         { return nil }
func (r *Runner) InstanceKey() string          { return r.localRouteKey }
func (r *Runner) KeysAPI() etcd.KeysAPI        { return etcd.NewKeysAPI(r.client) }
func (r *Runner) PathRoot() string             { return ServiceRoot }
func (r *Runner) Replicas() int                { return r.replicaCount }
func (r *Runner) ItemState(item string) string { return "ready" }

func (r *Runner) ItemIsReadyForPromotion(item, state string) bool {
	name, err := itemToJournal(item)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "item": item}).
			Error("failed to decode journal")
		return false
	}

	// Peer is ready for promotion iff |item| is locally brokered, and has served
	// a Append operation. This implies the current toplogy has served at least
	// one successful two-phase commit, and that all replicas are thus consistent.
	return r.router.HasServedAppend(name)
}

func (r *Runner) ItemRoute(item string, route allocator.IRoute, index int, tree *etcd.Node) {
	defer func(start time.Time) {
		var s = time.Since(start).Seconds()
		metrics.ItemRouteDurationSeconds.Observe(s)
	}(time.Now())

	var name, err = itemToJournal(item)
	if err != nil {
		log.WithFields(log.Fields{"err": err, "item": item}).
			Error("failed to decode journal")
		return
	}
	token, err := routeToToken(route)
	if err != nil {
		log.WithFields(log.Fields{"route": route, "err": err}).
			Error("failed to extract route token")
		return
	}

	r.router.transition(name, token, index, r.replicaCount)
}

func itemToJournal(s string) (journal.Name, error) {
	s, err := url.QueryUnescape(s)
	return journal.Name(s), err
}

func journalToItem(j journal.Name) string {
	return url.QueryEscape(string(j))
}

// Converts a unique allocator.IRoute into a correponding journal.RouteToken.
// In particular, given a route of parent `/path/to/item` and ordered Entries
// `/path/to/item/http%3A%2F%2Ffoo` & `/path/to/item/http%3A%2F%2Fbar`, returns
// RouteToken `http://foo|http://bar`.
func routeToToken(rt allocator.IRoute) (journal.RouteToken, error) {
	var buf bytes.Buffer
	var prefix = len(rt.Item2().Key) + 1

	if len(rt.Entries2()) == 0 {
		return "", nil
	}

	for i := range rt.Entries2() {
		if url, err := url.QueryUnescape(rt.Entries2()[i].Key[prefix:]); err != nil {
			return "", err
		} else {
			buf.WriteString(url)
			buf.WriteByte('|')
		}
	}
	return journal.RouteToken(buf.Bytes()[:buf.Len()-1]), nil // Trim trailing '|'.
}
