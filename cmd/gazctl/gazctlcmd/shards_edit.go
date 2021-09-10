package gazctlcmd

import (
	"bytes"
	"context"
	"io"

	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd/editor"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/shardspace"
	mbp "go.gazette.dev/core/mainboilerplate"
	"gopkg.in/yaml.v2"
)

type cmdShardsEdit struct {
	EditConfig
}

func init() {
	ShardRegisterCommands = append(ShardRegisterCommands, AddCmdShardsEdit)
}

func AddCmdShardsEdit(cmd *flags.Command) error {
	_, err := cmd.AddCommand("edit", "Edit shard specifications", shardsEditLongDesc, &cmdShardsEdit{})
	return err
}

func (cmd *cmdShardsEdit) Execute([]string) error {
	startup()
	return editor.EditRetryLoop(editor.RetryLoopArgs{
		FilePrefix:       "gazctl-shards-edit-",
		SelectFn:         cmd.selectSpecs,
		ApplyFn:          cmd.applyShardSpecYAML,
		AbortIfUnchanged: true,
	})
}

func (cmd *cmdShardsEdit) selectSpecs() io.Reader {
	var resp = listShards(cmd.Selector)

	var buf = &bytes.Buffer{}
	if len(resp.Shards) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no shards match selector")
	}
	writeHoistedYAMLShardSpace(buf, resp)

	return buf
}

func (cmd *cmdShardsEdit) applyShardSpecYAML(b []byte) error {
	var set shardspace.Set
	if err := yaml.UnmarshalStrict(b, &set); err != nil {
		return err
	}

	var ctx = context.Background()
	var req = newShardSpecApplyRequest(set)

	if err := req.Validate(); err != nil {
		return err
	} else if err = consumer.VerifyReferencedJournals(ctx, ShardsCfg.Broker.MustJournalClient(ctx), req); err != nil {
		return errors.WithMessage(err, "verifying referenced journals")
	}

	var resp, err = consumer.ApplyShardsInBatches(ctx, ShardsCfg.Consumer.MustShardClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply shards")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	return nil
}
