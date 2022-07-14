package gazctlcmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pb "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdRecoverylogPlay struct {
	Name string `long:"name" short:"n" required:"true" description:"Shard name"`
	Dir  string `long:"dir" short:"d" required:"true" description:"Directory to write the played recovery log into"`
}

func init() {
	CommandRegistry.AddCommand("shards", "recoverylog-play", "Play the shard recoverylog", `
Play the shard recoverylog into a directory.

Given a shard name, reads the shard recovery logs and plays them using a recoverylog.Player,
writing the played logs into a chosen directory.

Examples:

# Play a recovery log into logs directory
gazctl shards recoverylog-play --shard=your-journal/00000000-00000000 --dir=logs
`, &cmdRecoverylogPlay{})
}

func (cmd *cmdRecoverylogPlay) Execute([]string) error {
	startup(ShardsCfg.BaseConfig)

	// Install a signal handler which cancels a top-level |ctx|.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	var ctx, cancel = context.WithCancel(context.Background())
	go func() {
		<-signalCh
		cancel()
	}()

	var resp, err = consumer.FetchHints(ctx, ShardsCfg.Consumer.MustShardClient(ctx), &pb.GetHintsRequest{
		Shard: pb.ShardID(cmd.Name),
	})
	mbp.Must(err, "failed to fetch hints for shard")

	var rjc = ShardsCfg.Broker.MustRoutedJournalClient(ctx)
	var ajc = client.NewAppendService(ctx, rjc)
	var player = recoverylog.NewPlayer()
	player.FinishAtWriteHead()
	err = player.Play(ctx, *resp.PrimaryHints.Hints, cmd.Dir, ajc)
	mbp.Must(err, "failed to play recoverylog")

	return nil
}
