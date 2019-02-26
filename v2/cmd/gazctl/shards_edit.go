package main

import (
	"bytes"
	"context"
	"io"

	"github.com/LiveRamp/gazette/v2/cmd/gazctl/editor"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumer/shardspace"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdShardsEdit struct {
	EditConfig
}

func init() {
	_ = mustAddCmd(cmdShards, "edit", "Edit shard specifications", shardsEditLongDesc, &cmdShardsEdit{})
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
	var req = newShardSpecApplyRequest(set)
	if err := req.Validate(); err != nil {
		return err
	}

	var ctx = context.Background()
	var resp, err = consumer.ApplyShardsInBatches(ctx, shardsCfg.Consumer.ShardClient(ctx), req, cmd.MaxTxnSize)
	mbp.Must(err, "failed to apply shards")
	log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	return nil
}
