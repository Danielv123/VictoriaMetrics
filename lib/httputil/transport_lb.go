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
	"sync"
	"time"

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
	t.discoverBackendsLocked(context.Background())
	return t
}

type loadbalancerTransport struct {
	tr   http.RoundTripper
	host string
	port string

	// mu protects fields below
	mu             sync.Mutex
	lastResolvedAt time.Time
	dbs            *discoveredBackends
}

type discoveredBackends struct {
	backends []string
	idx      uint64
}

// RoundTrip implements http.RoundTripper interface
func (lb *loadbalancerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	backend := lb.pickBackend(r.Context(), false)
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
			backend := lb.pickBackend(r.Context(), true)
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

func (lb *loadbalancerTransport) pickBackend(ctx context.Context, forceDiscovery bool) string {
	ct := time.Now()
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if forceDiscovery && !ct.Before(lb.lastResolvedAt) {
		// prevent concurrent force discovery
		lb.lastResolvedAt = time.Time{}
	}

	if lb.dbs == nil || ct.Sub(lb.lastResolvedAt) > 30*time.Second {
		lb.discoverBackendsLocked(ctx)
	}
	if lb.dbs == nil || len(lb.dbs.backends) == 0 {
		return ""
	}
	idx := lb.dbs.idx
	lb.dbs.idx++
	return lb.dbs.backends[idx%uint64(len(lb.dbs.backends))]
}

func (lb *loadbalancerTransport) discoverBackendsLocked(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	addrs, err := netutil.Resolver.LookupIPAddr(ctx, lb.host)
	if err != nil {
		logger.Errorf("cannot discover ips for host: %q: %s", lb.host, err)
		return
	}
	backends := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if !netutil.TCP6Enabled() {
			ip, ok := netip.AddrFromSlice(addr.IP)
			if !ok {
				logger.Panicf("BUG: cannot build netip Addr from slice addr: %q", addr.IP.String())
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
	lb.dbs = dbs
	lb.lastResolvedAt = time.Now()
}
