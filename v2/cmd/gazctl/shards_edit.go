package main

import (
	"bytes"
	"context"
	"io"

	"github.com/LiveRamp/gazette/v2/cmd/gazctl/editor/editor"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdShardsEdit struct {
	EditConfig
}

func (cmd *cmdShardsEdit) Execute([]string) error {
	startup()
	return editor.EditRetryLoop("gazctl-shards-edit-", cmd.selectSpecs, applyShardSpecYAML)
}

func (cmd *cmdShardsEdit) selectSpecs() io.Reader {
	var resp = listShards(cmd.Selector)

	var buf = &bytes.Buffer{}
	if len(resp.Shards) == 0 {
		logrus.WithField("selector", cmd.Selector).Panic("no shards match selector")
		panic("unreachable")
	}
	writeYAMLShardSpec(buf, resp)

	return buf
}

func applyShardSpecYAML(b []byte) error {

	var shards []yamlShard
	if err := yaml.UnmarshalStrict(b, &shards); err != nil {
		return err
	}

	var req = newShardSpecApplyRequest(shards)
	if err := req.Validate(); err != nil {
		return err
	}

	var ctx = context.Background()
	if resp, err := consumer.ApplyShards(ctx, shardsCfg.Consumer.ShardClient(ctx), req); err != nil {
		return err
	} else {
		log.WithField("rev", resp.Header.Etcd.Revision).Info("successfully applied")
	}

	return nil
}
