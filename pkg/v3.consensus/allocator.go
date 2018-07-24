package v3_consensus

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/mirror"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	LeaseTTL   time.Duration
	Root       string
	LocalRoute string
}

const (
	MembersPrefix = "members"
	ItemsPrefix   = "items"
	RoutesPrefix  = "routes"
)

type KeyValue struct {
	mvccpb.KeyValue
	// Decoded is the cached, user-defined decoding of |Value|.
	Decoded interface{}
}

type KeyValues []KeyValue

func (kvs *KeyValues) Patch(decoder func(key, value []byte) (interface{}, error), events ...*clientv3.Event) error {
}

type Member interface {
	ItemLimit() int
	RackLocation() string
	LeaseID() clientv3.Lease
	Address() url.URL
}

type Item interface {
	DesiredReplication() int
}

type Assignment struct {
	Item      []byte
	Leader    []byte
	Followers [][]byte
}

type Snapshot struct {
	// Etcd cluster revision which this View reflects.
	Revision int64
	Root     string

	// Current consensus members.
	Members KeyValues
	// Current consensus items.
	Items KeyValues
	// Assignments of items to members.
	Assignments map[string]Assignment
}

func Allocate(ctx context.Context, client *clientv3.Client, cfg Config) error {
	// Acquire an Etcd lease having the lifetime of this Allocate invocation.
	var lessor = clientv3.NewLease(client)
	defer lessor.Close()

	var lease, err = lessor.Grant(ctx, int64(cfg.LeaseTTL.Seconds()))
	if err != nil {
		return err
	} else if lease.Error != "" {
		// TODO(johnny): This appears vestigial in the API?
		return fmt.Errorf("granting lease: %s", lease.Error)
	} else if _, err = lessor.KeepAlive(ctx, lease.ID); err != nil {
		return err
	}

	var kv = clientv3.NewKV(client)

	var syncer = mirror.NewSyncer(client, cfg.Root, 0)

	for respCh, errCh := syncer.SyncBase(ctx); true; {
		select {
		case resp := <-respCh:
			applyKeyValues(resp.Kvs)
			break
		case err := <-errCh:
			return err
		}
	}
	var watchCh = syncer.SyncUpdates(ctx)

	return nil
}

/*
func createMemberKey(ctx context.Context, kv clientv3.KV, cfg Config, lease *clientv3.LeaseGrantResponse) error {
	var key = path.Join(cfg.Root, MembersPrefix, cfg.LocalRoute)
	var value = strconv.FormatInt(int64(lease.ID), 10)

	for {
		var txnResp, err = kv.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", 0)).
			Then(clientv3.OpPut(key, value,
				clientv3.WithLease(lease.ID))).
			Commit()

		if err != nil {
			return err
		} else if txnResp.Succeeded {
			break
		}

		log.WithField("key", key).Warn("waiting for prior member key to expire")
		select {
		case <-time.After(time.Second * 10):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

}
*/
