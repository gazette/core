// Package etcdtest provides test support for obtaining a client to an Etcd server.
//
package etcdtest

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// TestClient returns a client of the embeded Etcd test server. It asserts that
// the Etcd keyspace is empty before returing to the client. In other words,
// it asserts that the prior test cleaned up after itself.
func TestClient() *clientv3.Client {
	var resp, err = _etcdClient.Get(context.Background(), "", clientv3.WithPrefix(), clientv3.WithLimit(5))
	if err != nil {
		log.Fatal(err)
	} else if len(resp.Kvs) != 0 {
		log.Fatalf("etcd not empty; did a previous test not clean up?\n%+v", resp)
	}
	return _etcdClient
}

// Cleanup is called at the completion of each test using TestClient,
// to remove any remaining key/value fixtures in the Etcd store.
func Cleanup() {
	if _, err := _etcdClient.Delete(context.Background(), "", clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}
}

var (
	_cmd        *exec.Cmd
	_etcdClient *clientv3.Client
)

// TestMainWithEtcd is to be called by other packages which require
// functionality of the etcdtest package, before those tests run, as:
//
//   func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
//
// This TestMain function is automatically invoked by the `go test`
// tool, providing an opportunity to start the embedded Etcd server
// prior to test invocations.
func TestMainWithEtcd(m *testing.M) {
	_cmd = exec.Command("etcd",
		"--log-level", "error",
		"--listen-peer-urls", "unix://peer.sock:0",
		"--listen-client-urls", "unix://client.sock:0",
		"--advertise-client-urls", "unix://client.sock:0",
	)
	log.Println("Starting etcd: ", _cmd.Args)

	var err error
	if _cmd.Dir, err = ioutil.TempDir("", "etcdtest"); err != nil {
		log.Fatal(err)
	}
	_cmd.Stdout = os.Stdout
	_cmd.Stderr = os.Stderr

	if err = _cmd.Start(); err != nil {
		log.Fatal(err)
	}

	os.Exit(func() int {
		// Defer Etcd tear-down.
		defer func() {
			if err = _cmd.Process.Signal(syscall.SIGTERM); err != nil {
				log.Fatal("failed to TERM etcd: ", err)
			}
			_ = _cmd.Wait()

			if err = os.RemoveAll(_cmd.Dir); err != nil {
				log.Fatalf("failed to remove etcd tmp directory %v: %v", _cmd.Dir, err)
			}
		}()

		// Build client.
		var ep = "unix://" + _cmd.Dir + "/client.sock:0"
		log.Println("using test endpoint: " + ep)

		if _etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout: 5 * time.Second,
		}); err != nil {
			log.Fatal(err)
		}
		// Verify test client works.
		_ = TestClient()

		// Run tests.
		return m.Run()
	}())
}
