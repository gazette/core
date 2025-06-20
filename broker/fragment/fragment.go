package fragment

import (
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
)

// Fragment wraps the protocol.Fragment type with a nil-able backing local File.
type Fragment struct {
	protocol.Fragment
	// Local uncompressed file of the Fragment, or nil iff the Fragment is remote.
	File stores.File
}
