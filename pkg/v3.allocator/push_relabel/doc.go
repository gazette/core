// Package push_relabel is an implementation of the maximum-flow algorithm of the
// same name, using the highest-label selection rule and further extended to
// incorporate a notion of Arc "priorities". Priorities are applied to greedily
// select among admissible arcs during a Node discharge step. Note that since
// push/relabel specifies no order over admissible arcs, this does not change
// the properties of the algorithm.
//
// Priorities allow for efficient incremental maximum-flow solutions which
// relate closely to prior flow-network solutions (eg, by prioritizing arcs
// of the prior solution). However, this is a greedy algorithm, and there is
// no guarantee of optimality (as there would be with min-cost / max-flow).
package push_relabel
