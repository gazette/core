package gazctlcmd

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

type cmdShardsUnassign struct {
	DryRun   bool   `long:"dry-run" description:"Perform a dry-run, printing matching shards"`
	Selector string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
}

func init() {
	CommandRegistry.AddCommand("shards", "unassign", "Remove a shard assignment", `
Removes the assignment of a shard from its primary consumer process, causing it
to be shortly rescheduled by the allocator. This can be used to restart a failed
shard, or to move a shard off an overloaded node.

Use --selector to supply a LabelSelector which constrains the set of returned
shards. Shard selectors support an additional meta-label "id". See the 'shards
list' command for more details about label selectors.
`, &cmdShardsUnassign{})
}

func (cmd *cmdShardsUnassign) Execute([]string) (err error) {
	startup(ShardsCfg.BaseConfig)

	var listResp = listShards(cmd.Selector)
	if listResp.Status != pc.Status_OK {
		return fmt.Errorf("unexpected listShard status: %v", listResp.Status.String())
	}
	var shards = listResp.Shards

	if !cmd.DryRun {
		var ctx = context.Background()
		var client = pc.NewShardClient(ShardsCfg.Consumer.MustDial(ctx))

		for _, shard := range shards {
			resp, err := client.Unassign(pb.WithDispatchDefault(ctx), &pc.UnassignRequest{Shard: shard.Spec.Id})
			if err != nil {
				return fmt.Errorf("unassigning shard: %w", err)
			} else if err := resp.Validate(); err != nil {
				return fmt.Errorf("invalid response: %w", err)
			}
		}
	}

	if len(shards) > 0 {
		log.Infof("Successfully unassigned %v shards: %v", len(shards), shardIds(shards))
	} else {
		log.Warn("No shards assignments were modified. Use `--dry-run` to test your selector.")
	}

	return nil
}

func shardIds(shards []pc.ListResponse_Shard) []string {
	var ids = make([]string, len(shards))

	for _, shard := range shards {
		ids = append(ids, shard.Spec.Id.String())
	}

	return ids
}
