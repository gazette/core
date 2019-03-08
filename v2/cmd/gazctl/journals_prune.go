package main

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

type cmdJournalsPrune struct {
	pruneConfig
}

func init() {
	_ = mustAddCmd(cmdJournals, "prune", "Deletes fragments older than the configured retention", `
Deletes fragments across all configured fragment stores of matching journals that are older than the configured retention.

There is a caveat when pruning journals. Only fragments that are part of the "blessed" history are pruned in a given pass. Fragments associated to dead end forks will not be deleted. As a workaround, operators can wait for the fragment listing to refresh and prune the journals again.

Use --selector to supply a LabelSelector to select journals to prune. See "journals list --help" for details and examples.
`, &cmdJournalsPrune{})
}

func (cmd *cmdJournalsPrune) Execute([]string) error {
	startup()

	var resp = listJournals(cmd.Selector)
	if len(resp.Journals) == 0 {
		log.WithField("selector", cmd.Selector).Panic("no journals match selector")
	}

	var now = time.Now()
	for _, j := range resp.Journals {
		for _, f := range fetchAgedFragments(j.Spec, now) {
			var spec = f.Spec
			log.WithFields(log.Fields{
				"journal": spec.Journal,
				"name":    spec.ContentName(),
				"size":    spec.ContentLength(),
				"mod":     spec.ModTime,
			}).Info("pruning fragment")

			if !cmd.DryRun {
				err := fragment.Remove(context.Background(), spec)
				mbp.Must(err, "error removing fragment", "path", spec.ContentPath())
			}
		}
	}
	return nil
}

// fetchAgedFragments returns fragments of the journal that are older than the
// configured retention.
func fetchAgedFragments(spec pb.JournalSpec, now time.Time) []pb.FragmentsResponse__Fragment {
	var ctx = context.Background()
	var jc = journalsCfg.Broker.RoutedJournalClient(ctx)
	resp, err := client.ListAllFragments(ctx, jc, pb.FragmentsRequest{Journal: spec.Name})
	mbp.Must(err, "failed to fetch fragments")

	var retention = spec.Fragment.Retention

	var aged = make([]pb.FragmentsResponse__Fragment, 0)
	for _, f := range resp.Fragments {
		if f.Spec.BackingStore == "" {
			continue
		}
		var age = now.Sub(time.Unix(f.Spec.ModTime, 0))
		if age >= retention {
			aged = append(aged, f)
		}
	}

	log.WithFields(log.Fields{
		"total": len(resp.Fragments),
		"aged":  len(aged),
	}).Info("fetched aged fragments")

	return aged
}
