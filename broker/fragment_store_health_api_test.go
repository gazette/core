package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/broker/stores/fs"
	"go.gazette.dev/core/etcdtest"
)

func TestFragmentStoreHealthCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Save and restore providers and file system root
	var prevProviders = stores.GetProviders()
	defer stores.RegisterProviders(prevProviders)

	var prevRoot = fs.FileSystemStoreRoot
	defer func() { fs.FileSystemStoreRoot = prevRoot }()
	fs.FileSystemStoreRoot = t.TempDir()
	defer stores.Sweep() // Stop & remove stores after the test.

	// Register the file store provider
	stores.RegisterProviders(map[string]stores.Constructor{
		"file": fs.New,
	})
	require.NoError(t, os.MkdirAll(filepath.Join(fs.FileSystemStoreRoot, "subdir"), 0755))

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: health of a valid FragmentStore.
	var resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "file:///subdir/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Empty(t, resp.StoreHealthError)
	require.NotNil(t, resp.Header)

	// Case: health of an invalid FragmentStore.
	resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "file:///does/not/exist/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_FRAGMENT_STORE_UNHEALTHY, resp.Status)
	require.Contains(t, resp.StoreHealthError, "invalid file store directory")

	// Case: context is canceled.
	ctx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately before the call

	_, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "file:///also/does/not/exist/",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
}
