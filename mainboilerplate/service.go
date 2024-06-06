package mainboilerplate

import (
	petname "github.com/dustinkirkland/golang-petname"
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/server"
)

// ZoneConfig configures the zone of the application.
type ZoneConfig struct {
	Zone string `long:"zone" env:"ZONE" default:"local" description:"Availability zone within which this process is running"`
}

// ServiceConfig represents identification and addressing configuration of the process.
type ServiceConfig struct {
	ZoneConfig
	ID             string `long:"id" env:"ID" description:"Unique ID of this process. Auto-generated if not set"`
	Host           string `long:"host" env:"HOST" description:"Addressable, advertised hostname or IP of this process. Hostname is used if not set"`
	Port           string `long:"port" env:"PORT" description:"Service port for HTTP and gRPC requests. A random port is used if not set. Port may also take the form 'unix:///path/to/socket' to use a Unix Domain Socket"`
	ServerCertFile string `long:"server-cert-file" env:"SERVER_CERT_FILE" default:"" description:"Path to the server TLS certificate. This option toggles whether TLS is used. If absent, all other TLS settings are ignored."`
	ServerKeyFile  string `long:"server-key-file" env:"SERVER_KEY_FILE" default:"" description:"Path to the server TLS private key"`
	ServerCAFile   string `long:"server-ca-file" env:"SERVER_CA_FILE" default:"" description:"Path to the trusted CA for server verification of client certificates. When present, client certificates are required and verified against this CA. When absent, client certificates are not required but are verified against the system CA pool if presented."`
	PeerCertFile   string `long:"peer-cert-file" env:"PEER_CERT_FILE" default:"" description:"Path to the client TLS certificate for peer-to-peer requests"`
	PeerKeyFile    string `long:"peer-key-file" env:"PEER_KEY_FILE" default:"" description:"Path to the client TLS private key for peer-to-peer requests"`
	PeerCAFile     string `long:"peer-ca-file" env:"PEER_CA_FILE" default:"" description:"Path to the trusted CA for client verification of peer server certificates. When absent, the system CA pool is used instead."`
}

// ProcessSpec of the ServiceConfig.
func (cfg ServiceConfig) BuildProcessSpec(srv *server.Server) protocol.ProcessSpec {
	if cfg.ID == "" {
		cfg.ID = petname.Generate(2, "-")
	}

	return protocol.ProcessSpec{
		Id:       protocol.ProcessSpec_ID{Zone: cfg.Zone, Suffix: cfg.ID},
		Endpoint: srv.Endpoint(),
	}
}
