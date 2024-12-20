package gazctlcmd

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/task"
)

type cmdJournalSuspend struct {
	Selector    string `long:"selector" short:"l" required:"true" description:"Label Selector query to filter on"`
	Replication int32  `long:"replication" short:"r" required:"true" description:"Desired replication after suspension. Typically zero or one"`
}

func init() {
	CommandRegistry.AddCommand("journals", "suspend", "Suspend idle journals", `
Suspend idle journals to use fewer or no broker resources.

Suspended journals are read-only and typically have a lower replication
factor then usual: often one, or even zero if reads are not needed.

Ordinarily lowering the replication factor of a journal risks loss of journal
offset consistency. Suspension avoids this by stashing the journal write head
in the JournalSpec itself. Later, a journal is resumed by increasing its
replication factor and marking it as write-able. Broker replicas then apply the
stashed append bound to lower-bound the offset at which appends resume.
`, &cmdJournalSuspend{})
}

func (cmd *cmdJournalSuspend) Execute([]string) error {
	startup(JournalsCfg.BaseConfig)

	var err error
	var ctx = context.Background()
	var rjc = JournalsCfg.Broker.MustRoutedJournalClient(ctx)

	// Get the list of journals which match this selector.
	var listRequest pb.ListRequest
	listRequest.Selector, err = pb.ParseLabelSelector(cmd.Selector)
	mbp.Must(err, "failed to parse label selector", "selector", cmd.Selector)

	var listResp *pb.ListResponse
	listResp, err = client.ListAllJournals(ctx, rjc, listRequest)
	mbp.Must(err, "failed to resolved journals from selector", cmd.Selector)

	var tasks = task.NewGroup(ctx)
	for _, journal := range listResp.Journals {
		tasks.Queue(journal.Spec.Name.String(), func() error {
			var bound, err = suspendJournal(tasks.Context(), rjc, journal.Spec, journal.ModRevision, cmd.Replication)
			if err != nil {
				return err
			}

			logrus.WithFields(logrus.Fields{
				"journal": journal.Spec.Name,
				"offset":  bound,
			}).Info("suspended journal")

			return nil
		})
	}

	tasks.GoRun()
	return tasks.Wait()
}

func suspendJournal(
	ctx context.Context,
	rjc pb.RoutedJournalClient,
	spec pb.JournalSpec,
	modRevision int64,
	replication int32,
) (int64, error) {

	// Mark journal as read-only to fence off any potential raced writes.
	spec.Flags = pb.JournalSpec_O_RDONLY

	var applyResp, err = client.ApplyJournals(ctx, rjc, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				ExpectModRevision: modRevision,
				Upsert:            &spec,
			},
		},
	})
	if err != nil {
		return 0, err
	}
	modRevision = applyResp.Header.Etcd.Revision

	// Perform a transactional read of the current write-head,
	// and modify `spec` accordingly.
	var lastHeader *pb.Header

	for {
		var appendResp, err = client.Append(ctx, rjc, pb.AppendRequest{
			Header:  lastHeader,
			Journal: spec.Name,
		})

		if err != nil {
			return 0, err
		}

		if appendResp.Header.Etcd.Revision < modRevision {
			// Tell primary it must read through `modRevision` before responding.
			lastHeader = &appendResp.Header
			lastHeader.Etcd.Revision = modRevision
			continue
		}

		spec.AppendBound = appendResp.Commit.End
		spec.Replication = replication
		break
	}

	_, err = client.ApplyJournals(ctx, rjc, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				ExpectModRevision: modRevision,
				Upsert:            &spec,
			},
		},
	})
	return spec.AppendBound, err
}
