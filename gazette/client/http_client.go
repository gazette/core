package client

import "net/http"

//go:generate mockery -name=HttpClient

type HttpClient interface {
	Do(*http.Request) (*http.Response, error)
	Get(url string) (*http.Response, error)
}
