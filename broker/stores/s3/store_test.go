package s3

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// TestS3StoreIsAuthError tests S3 store auth error classification
func TestS3StoreIsAuthError(t *testing.T) {
	store := &store{}

	// 403 Forbidden wrapped as the v2 SDK delivers it: an awshttp.ResponseError
	// whose embedded smithyhttp.ResponseError carries the http.Response.
	forbidden := &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: http.StatusForbidden},
			},
			Err: &smithy.GenericAPIError{Code: "Forbidden", Message: "Forbidden"},
		},
		RequestID: "request-id",
	}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "NoSuchBucket error should be auth error",
			err:      &types.NoSuchBucket{Message: aws.String("The specified bucket does not exist")},
			expected: true,
		},
		{
			name:     "AccessDenied error should be auth error",
			err:      &smithy.GenericAPIError{Code: s3ErrCodeAccessDenied, Message: "Access Denied"},
			expected: true,
		},
		{
			name:     "403 Forbidden via ResponseError should be auth error",
			err:      forbidden,
			expected: true,
		},
		{
			name:     "InvalidAccessKeyId should not be auth error (AuthN failure)",
			err:      &smithy.GenericAPIError{Code: "InvalidAccessKeyId", Message: "The AWS Access Key Id you provided does not exist"},
			expected: false,
		},
		{
			name:     "Generic error should not be auth error",
			err:      errors.New("connection timeout"),
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
