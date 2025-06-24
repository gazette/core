package fs

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFSStoreIsAuthError tests filesystem store auth error classification
func TestFSStoreIsAuthError(t *testing.T) {
	store := &store{}

	// Test cases for filesystem auth errors
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Permission denied should be auth error",
			err:      &os.PathError{Op: "open", Path: "/protected/file", Err: os.ErrPermission},
			expected: true,
		},
		{
			name:     "File not exist (current directory) should not be auth error",
			err:      &os.PathError{Op: "open", Path: "file.txt", Err: os.ErrNotExist},
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("disk full"),
			expected: false,
		},
		{
			name:     "Nil error should not be auth error",
			err:      nil,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := store.IsAuthError(test.err)
			require.Equal(t, test.expected, result)
		})
	}
}
