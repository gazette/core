package protocol

import (
	"net/url"
)

// Endpoint defines an accessible service address. It is a URL, where the
// scheme defines the network transport and semantics of the host, path,
// and query components. At present, supported schemes are:
//
//   - http://host(:port)/path?query
//   - unix://host/absolute/path/to/unix/domain/socket
type Endpoint string

// Validate returns an error if the Endpoint is not well-formed.
func (ep Endpoint) Validate() error {
	var _, err = ep.parse()
	return err
}

// URL returns the Endpoint as a URL. The Endpoint must Validate, or URL panics.
func (ep Endpoint) URL() *url.URL {
	if url, err := ep.parse(); err == nil {
		return url
	} else {
		panic(err.Error())
	}
}

// GRPCAddr maps this Endpoint into an address form suitable for gRPC to dial.
func (ep Endpoint) GRPCAddr() string {
	// gRPC wants the host/authority of a unix:// URL to be empty,
	// whereas we populate it with the hostname. Strip it.
	var addr string
	if u := ep.URL(); u.Scheme == "unix" {
		addr = "unix://" + u.Path
	} else if u.Port() == "" && u.Scheme == "https" {
		addr = u.Host + ":443"
	} else if u.Port() == "" && u.Scheme == "http" {
		addr = u.Host + ":80"
	} else {
		addr = u.Host
	}
	return addr
}

func (ep Endpoint) parse() (*url.URL, error) {
	var url, err = url.Parse(string(ep))
	if err != nil {
		return nil, &ValidationError{Err: err}
	} else if !url.IsAbs() {
		return nil, NewValidationError("not absolute: %s", ep)
	} else if url.Host == "" {
		return nil, NewValidationError("missing host: %s", ep)
	}
	return url, nil
}
