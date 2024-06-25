package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// NewKeyedAuth returns an Authorizer and Verifier using the given pre-shared
// secret keys, which are base64 encoded and separate by whitespace and/or commas.
// The first key is used for signing Authorizations, and any key may verify
// a presented Authorization.
func NewKeyedAuth(encodedKeys string) (interface {
	pb.Authorizer
	pb.Verifier
}, error) {
	var keys jwt.VerificationKeySet

	for i, key := range strings.Fields(strings.ReplaceAll(encodedKeys, ",", " ")) {
		if b, err := base64.StdEncoding.DecodeString(key); err != nil {
			return nil, fmt.Errorf("failed to decode key at index %d: %w", i, err)
		} else {
			keys.Keys = append(keys.Keys, b)
		}
	}
	return &keySet{keys}, nil
}

type keySet struct {
	jwt.VerificationKeySet
}

func (k *keySet) Authorize(ctx context.Context, claims pb.Claims, exp time.Duration) (context.Context, error) {
	var now = time.Now()
	claims.IssuedAt = &jwt.NumericDate{Time: now}
	claims.ExpiresAt = &jwt.NumericDate{Time: now.Add(exp)}
	var token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(k.Keys[0])

	if err != nil {
		return ctx, err
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", token)), nil
}

func (k *keySet) Verify(ctx context.Context, require pb.Capability) (context.Context, context.CancelFunc, pb.Claims, error) {
	if claims, err := verifyWithKeys(ctx, require, k.VerificationKeySet); err != nil {
		return ctx, func() {}, claims, status.Error(codes.Unauthenticated, err.Error())
	} else {
		ctx, cancel := context.WithDeadline(ctx, claims.ExpiresAt.Time)
		return ctx, cancel, claims, nil
	}
}

// NewNoopAuth returns an Authorizer and Verifier which does nothing.
func NewNoopAuth() interface {
	pb.Authorizer
	pb.Verifier
} {
	return &noop{}
}

type noop struct{}

func (k *noop) Authorize(ctx context.Context, claims pb.Claims, exp time.Duration) (context.Context, error) {
	return ctx, nil
}
func (v *noop) Verify(ctx context.Context, require pb.Capability) (context.Context, context.CancelFunc, pb.Claims, error) {
	return ctx, func() {}, pb.Claims{Capability: require}, nil
}

func verifyWithKeys(ctx context.Context, require pb.Capability, keys jwt.VerificationKeySet) (pb.Claims, error) {
	var md, _ = metadata.FromIncomingContext(ctx)
	var auth = md.Get("authorization")

	if len(auth) == 0 {
		return errClaims, ErrMissingAuth
	} else if !strings.HasPrefix(auth[0], "Bearer ") {
		return errClaims, ErrNotBearer
	}
	var bearer = strings.TrimPrefix(auth[0], "Bearer ")
	var claims pb.Claims

	if token, err := jwt.ParseWithClaims(bearer, &claims,
		func(token *jwt.Token) (interface{}, error) { return keys, nil },
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(time.Second*5),
		jwt.WithValidMethods([]string{"HS256", "HS384"}),
	); err != nil {
		return errClaims, fmt.Errorf("verifying Authorization: %w", err)
	} else if !token.Valid {
		panic("token.Valid must be true")
	} else if err = verifyCapability(claims.Capability, require); err != nil {
		return errClaims, err
	} else {
		return claims, nil
	}
}

func verifyCapability(actual, require pb.Capability) error {
	if actual&require == require {
		return nil
	}

	// Nicer messages for common capabilities.
	for _, i := range []struct {
		cap  pb.Capability
		name string
	}{
		{pb.Capability_LIST, "LIST"},
		{pb.Capability_APPLY, "APPLY"},
		{pb.Capability_READ, "READ"},
		{pb.Capability_APPEND, "APPEND"},
		{pb.Capability_REPLICATE, "REPLICATE"},
	} {
		if require&i.cap != 0 && actual&i.cap == 0 {
			return fmt.Errorf("authorization is missing required %s capability", i.name)
		}
	}

	return fmt.Errorf("authorization is missing required capability (have %s, but require %s)",
		strconv.FormatUint(uint64(actual), 2), strconv.FormatUint(uint64(require), 2))
}

var (
	ErrMissingAuth = errors.New("missing or empty Authorization token")
	ErrNotBearer   = errors.New("invalid or unsupported Authorization header (expected 'Bearer')")

	// errClaims is a defense-in-depth sentinel LabelSelector that won't match anything,
	// just in case a caller fails to properly error-check a verification result.
	errClaims = pb.Claims{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("this-label-will", "never-match"),
		},
	}
)
