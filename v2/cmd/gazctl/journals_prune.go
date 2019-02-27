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
			log.WithFields(log.Fields{
				"journal": f.Journal,
				"name":    f.ContentName(),
				"size":    f.ContentLength(),
				"mod":     f.ModTime,
			}).Info("pruning fragment")

			if !cmd.DryRun {
				err := fragment.Remove(context.Background(), f.BackingStore, f.Fragment)
				mbp.Must(err, "error removing fragment", "path", f.ContentPath())
			}
		}
	}
	return nil
}

// fetchAgedFragments returns fragments of the journal that are older than the
// configured retention.
func fetchAgedFragments(j pb.JournalSpec, now time.Time) []pb.FragmentsResponse_SignedFragment {
	var ctx = context.Background()
	var jc = journalsCfg.Broker.RoutedJournalClient(ctx)
	resp, err := client.ListAllFragments(ctx, jc, pb.FragmentsRequest{Journal: j.Name})
	mbp.Must(err, "failed to fetch fragments")

	var retention = j.Fragment.Retention

	var aged = make([]pb.FragmentsResponse_SignedFragment, 0)
	for _, f := range resp.Fragments {
		var age = now.Sub(time.Unix(f.ModTime, 0))
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
