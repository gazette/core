package consumer

import (
	"context"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/recoverylog"
)

type replica struct {
	shard     ShardID
	player    *recoverylog.Player
	servingCh chan struct{} // Blocks until replica.serve exists.
}

func newReplica(shard *shard, runner *Runner, tree *etcd.Node) (*replica, error) {
	var hints, err = loadHintsFromEtcd(shard.id, runner, tree)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{"shard": shard.id, "hints": hints, "dir": shard.localDir}).
		Info("replicating with hints")

	player, err := recoverylog.NewPlayer(hints, shard.localDir)
	if err != nil {
		return nil, err
	}
	player.SetCancelChan(shard.cancelCh)

	return &replica{
		shard:     shard.id,
		player:    player,
		servingCh: make(chan struct{}),
	}, nil
}

func (r *replica) serve(runner *Runner) {
	defer close(r.servingCh)

	if err := r.player.Play(runner.Gazette); err != nil {
		switch err {
		case context.Canceled:
			// Do nothing, the shard is no longer being processed by this pod.
		default:
			log.WithFields(log.Fields{"shard": r.shard, "err": err}).Error("replication failed")
		}

		abort(runner, r.shard)
	} else {
		log.WithFields(log.Fields{"shard": r.shard}).Info("finished serving replica")
	}
}
