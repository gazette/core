package protocol

import (
	"net/url"
)

// FragmentStore defines a storage backend base path for Journal Fragments.
// It is a URL, where the scheme defines the storage backend service. As
// FragmentStores "root" remote storage locations of fragments, their path
// component must end in a trailing slash.
//
// Currently supported schemes are:
//
//  * gs://bucket-name/a/sub-path/?property=value
//  * s3://bucket-name/a/sub-path/?property=value
//  * file:///a/sub-path/?property=value
//
type FragmentStore string

// Validate returns an error if the FragmentStore is not well-formed.
func (fs FragmentStore) Validate() error {
	var _, err = fs.parse()
	return err
}

// URL returns the FragmentStore as a URL. The FragmentStore must Validate, or URL panics.
func (fs FragmentStore) URL() *url.URL {
	if url, err := fs.parse(); err == nil {
		return url
	} else {
		panic(err.Error())
	}
}

func (fs FragmentStore) parse() (*url.URL, error) {
	var url, err = url.Parse(string(fs))
	if err != nil {
		return nil, &ValidationError{Err: err}
	} else if !url.IsAbs() {
		return nil, NewValidationError("not absolute (%s)", fs)
	}

	switch url.Scheme {
	case "s3", "gs":
		if url.Host == "" {
			return nil, NewValidationError("missing bucket (%s)", fs)
		}
	case "file":
		if url.Host != "" {
			return nil, NewValidationError("file scheme cannot have host (%s)", fs)
		}
	default:
		return nil, NewValidationError("invalid scheme (%s)", url.Scheme)
	}

	if path := url.Path; path[len(path)-1] != '/' {
		return nil, NewValidationError("path component doesn't end in '/' (%s)", url.Path)
	}
	return url, nil
}

func fragmentStoresEq(a, b []FragmentStore) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
