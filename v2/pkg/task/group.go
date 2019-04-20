package task

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Group is a group of tasks which should each be executed concurrently,
// and which should be collectively blocked on until all are complete.
// Tasks should be preemptable, and the first task to return a non-nil
// error cancels the entire Group.
type Group struct {
	// Context of the Group, which is cancelled by:
	//  * Any function of the Group returning non-nil error, or
	//  * An explicit call to Cancel, or
	//  * A cancellation of the parent Context of the Group.
	//
	// Tasks queued to the Group should monitor Context and return
	// upon its cancellation.
	context.Context
	// Cancel cancels Context.
	Cancel context.CancelFunc

	tasks []task
	eg    *errgroup.Group
}

// task composes a runnable and its description.
type task struct {
	desc string
	fn   func() error
}

// NewGroup returns a new, empty Group with the given Context.
func NewGroup(ctx context.Context) *Group {
	ctx, cancel := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	return &Group{Context: ctx, eg: eg, Cancel: cancel}
}

// Queue a function for execution with the Group.
// Cannot be called after Start is invoked.
func (g *Group) Queue(desc string, fn func() error) {
	if len(g.tasks) == 0 && cap(g.tasks) != 0 {
		panic("Queue called after Start")
	}
	g.tasks = append(g.tasks, task{desc: desc, fn: fn})
}

// Start all queued functions.
func (g *Group) Start() {
	for i := range g.tasks {
		var t = g.tasks[i]
		g.eg.Go(func() error { return errors.WithMessage(t.fn(), t.desc) })
	}
	g.tasks = g.tasks[:0]

}

// Wait for started functions, returning only after all complete.
// The first encountered non-nil error is returned.
func (g *Group) Wait() error {
	if len(g.tasks) != 0 || cap(g.tasks) == 0 {
		panic("Wait called without prior Queue and Start")
	}
	return g.eg.Wait()
}
