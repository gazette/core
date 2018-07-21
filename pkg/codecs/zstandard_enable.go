// +build !windows

package codecs

import (
	"io"

	"github.com/DataDog/zstd"
)

func init() {
	zstdNewReader = zstd.NewReader
	zstdNewWriter = func(w io.Writer) io.WriteCloser { return zstd.NewWriter(w) }
}
