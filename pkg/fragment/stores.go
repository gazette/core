package fragment

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/gorilla/schema"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

func SignGetURL(fragment pb.Fragment, d time.Duration) (string, error) {
	var ep = fragment.BackingStore.URL()

	switch ep.Scheme {
	case "s3":
		return s3SignGET(ep, fragment, d)
	case "file":
		return fsURL(ep, fragment), nil
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

func Open(ctx context.Context, fragment pb.Fragment) (io.ReadCloser, error) {
	var ep = fragment.BackingStore.URL()

	switch ep.Scheme {
	case "s3":
		return s3Open(ctx, ep, fragment)
	case "file":
		return fsOpen(ep, fragment)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

func Persist(ctx context.Context, spool Spool) error {
	var ep = spool.Fragment.BackingStore.URL()

	switch ep.Scheme {
	case "s3":
		return s3Persist(ctx, ep, spool)
	case "file":
		return fsPersist(ep, spool)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

func List(ctx context.Context, store pb.FragmentStore, prefix string, callback func(pb.Fragment)) error {
	var ep = store.URL()

	switch ep.Scheme {
	case "s3":
		return s3List(ctx, store, ep, prefix, callback)
	case "file":
		return fsList(store, ep, prefix, callback)
	default:
		panic("unsupported scheme: " + ep.Scheme)
	}
}

func parseStoreArgs(ep *url.URL, args interface{}) error {
	var decoder = schema.NewDecoder()
	decoder.IgnoreUnknownKeys(false)

	if q, err := url.ParseQuery(ep.RawQuery); err != nil {
		return err
	} else if err = decoder.Decode(args, q); err != nil {
		return fmt.Errorf("parsing store URL arguments: %s", err)
	}
	return nil
}
