package protocol

import (
	gc "github.com/go-check/check"
)

type FragmentStoreSuite struct{}

func (s *FragmentStoreSuite) TestValidation(c *gc.C) {
	var cases = []struct {
		fs     FragmentStore
		expect string
	}{
		{"s3://my-bucket/?query", ""},         // Success.
		{"s3://my-bucket/subpath/?query", ""}, // Success (non-empty prefix).
		{"s3://my-bucket/subpath?query", `path component doesn't end in '/' \(/subpath\)`},
		{":garbage: :garbage:", "parse .* missing protocol scheme"},
		{"foobar://baz/", `invalid scheme \(foobar\)`},
		{"/baz/bing/", `not absolute \(/baz/bing/\)`},
		{"gs:///baz/bing/", `missing bucket \(gs:///baz/bing/\)`},
		{"file://host/baz/bing/", `file scheme cannot have host \(file://host/baz/bing/\)`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.fs.Validate(), gc.IsNil)
		} else {
			c.Check(tc.fs.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *FragmentStoreSuite) TestURLConversion(c *gc.C) {
	var fs FragmentStore = "s3://bucket/sub/path/?query"
	c.Check(fs.URL().Host, gc.Equals, "bucket")

	fs = "/baz/bing/"
	c.Check(func() { fs.URL() }, gc.PanicMatches, `not absolute \(/baz/bing/\)`)
}

var _ = gc.Suite(&FragmentStoreSuite{})
