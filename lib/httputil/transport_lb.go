package httputil

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
)

// NewLoadBalancerTransport returns new RoundTripper that performs round-robin HTTP requests loadbalancing
// based on discovered A dns records for the given url host
func NewLoadBalancerTransport(origin http.RoundTripper, url *url.URL) http.RoundTripper {
	host, port, err := net.SplitHostPort(url.Host)
	if err != nil {
		host = url.Host
	}
	t := &loadbalancerTransport{
		host: host,
		port: port,
		tr:   origin,
	}
	t.discoverBackends(context.Background())
	return t
}

type loadbalancerTransport struct {
	tr   http.RoundTripper
	host string
	port string

	discovering    atomic.Bool
	lastResolvedAt atomic.Uint64
	dbs            atomic.Pointer[discoveredBackends]
}

type discoveredBackends struct {
	backends []string
	idx      atomic.Uint64
}

// RoundTrip implements http.RoundTripper interface
func (lb *loadbalancerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	backend := lb.pickBackend(r.Context())
	if backend == "" {
		return nil, fmt.Errorf("no backends found for hostname=%q", lb.host)
	}

	r2 := r.Clone(r.Context())
	r2.URL.Host = backend
	if r2.Host == "" {
		r2.Host = r.URL.Host
	}
	resp, err := lb.tr.RoundTrip(r2)
	if err != nil {
		var dnsErr *net.DNSError
		// perform a single retry for in case of dns lookup error
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			lb.lastResolvedAt.Store(0)
			backend := lb.pickBackend(r.Context())
			if backend == "" {
				return nil, fmt.Errorf("no backends found for hostname=%q", lb.host)
			}

			r2 = r.Clone(r.Context())
			if r2.Host == "" {
				r2.Host = r.URL.Host
			}
			r2.URL.Host = backend
			resp, err = lb.tr.RoundTrip(r2)
		}
	}
	return resp, err
}

func (lb *loadbalancerTransport) pickBackend(ctx context.Context) string {
	dbs := lb.dbs.Load()
	if dbs == nil || fasttime.UnixTimestamp()-lb.lastResolvedAt.Load() > 30 {
		if newDBS := lb.discoverBackends(ctx); newDBS != nil {
			dbs = newDBS
		}
	}
	if dbs == nil || len(dbs.backends) == 0 {
		return ""
	}
	idx := dbs.idx.Add(1) - 1
	return dbs.backends[idx%uint64(len(dbs.backends))]
}

func (lb *loadbalancerTransport) discoverBackends(ctx context.Context) *discoveredBackends {
	// prevent concurrent dns lookup
	if !lb.discovering.CompareAndSwap(false, true) {
		return lb.dbs.Load()
	}
	defer lb.discovering.Store(false)

	addrs, err := netutil.Resolver.LookupIPAddr(ctx, lb.host)
	if err != nil {
		logger.Errorf("cannot discover ips for host: %q: %s", lb.host, err)
		return lb.dbs.Load()
	}
	backends := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if !netutil.TCP6Enabled() {
			ip, ok := netip.AddrFromSlice(addr.IP)
			if !ok {
				logger.Panicf("BUG: cannot build ip from slice addr: %q", addr.IP.String())
			}
			if !ip.Unmap().Is4() {
				continue
			}
		}
		ip := addr.IP.String()
		if len(lb.port) > 0 {
			ip = net.JoinHostPort(ip, lb.port)
		}
		backends = append(backends, ip)
	}
	rand.Shuffle(len(backends), func(i, j int) {
		backends[i], backends[j] = backends[j], backends[i]
	})
	dbs := &discoveredBackends{
		backends: backends,
	}
	lb.dbs.Store(dbs)
	lb.lastResolvedAt.Store(fasttime.UnixTimestamp())
	return dbs
}
