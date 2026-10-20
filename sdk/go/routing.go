package kokosync

import (
	"sort"

	"github.com/katteni-kaihatu/kokosync/protocol"
)

// RouteInfo describes transport submissions/receptions, not guaranteed datagram delivery.
// The Go QUIC adapter currently has no direct peer transport.
type RouteInfo struct {
	Peer           string   `json:"peer"`
	PublisherEpoch string   `json:"publisherEpoch"`
	Route          string   `json:"route"`
	Reason         string   `json:"reason"`
	RTTMs          *float64 `json:"rttMs"`
	SentDirect     uint64   `json:"sentDirect"`
	ReceivedDirect uint64   `json:"receivedDirect"`
	SentRelay      uint64   `json:"sentRelay"`
	ReceivedRelay  uint64   `json:"receivedRelay"`
}

func (c *Client) Routes() []RouteInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RouteInfo, 0, len(c.routes))
	for _, r := range c.routes {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}
func relayRoute(m Message) RouteInfo {
	return RouteInfo{Peer: protocol.String(m, "peer"), PublisherEpoch: protocol.String(m, "publisherEpoch"), Route: "relay", Reason: "local-adapter-unavailable"}
}
func routeEvent(r RouteInfo, removed bool) Message {
	return Message{"type": "route", "peer": r.Peer, "publisherEpoch": r.PublisherEpoch, "route": r.Route, "reason": r.Reason, "rttMs": nil, "sentDirect": r.SentDirect, "receivedDirect": r.ReceivedDirect, "sentRelay": r.SentRelay, "receivedRelay": r.ReceivedRelay, "removed": removed}
}
func (c *Client) trackRoutingLocked(m Message) Message {
	switch protocol.String(m, "type") {
	case "joined":
		c.info = m
		c.routes = map[string]RouteInfo{}
		if caps, ok := m["capabilities"].(map[string]any); ok {
			c.routingKnown, _ = caps["routeSignaling"].(bool)
		}
		if members, ok := m["peers"].([]any); ok {
			for _, member := range members {
				if p, ok := member.(map[string]any); ok {
					r := relayRoute(p)
					if r.Peer != "" && r.PublisherEpoch != "" {
						c.routes[r.Peer] = r
					}
				}
			}
		}
	case "peer_joined":
		if c.info == nil || protocol.String(m, "epoch") != protocol.String(c.info, "epoch") {
			return nil
		}
		r := relayRoute(m)
		if r.Peer == "" || r.PublisherEpoch == "" || r.Peer == protocol.String(c.info, "peer") {
			return nil
		}
		if _, exists := c.routes[r.Peer]; exists {
			return nil
		}
		c.routes[r.Peer] = r
		return routeEvent(r, false)
	case "peer_left":
		if c.info == nil || protocol.String(m, "epoch") != protocol.String(c.info, "epoch") {
			return nil
		}
		if r, exists := c.routes[protocol.String(m, "peer")]; exists && r.PublisherEpoch == protocol.String(m, "publisherEpoch") {
			delete(c.routes, r.Peer)
			return routeEvent(r, true)
		}
	}
	return nil
}
func (c *Client) validRealtimeLocked(m Message) bool {
	if !c.routingKnown {
		return true
	}
	peer := protocol.String(m, "peer")
	epoch := protocol.String(m, "publisherEpoch")
	if peer == protocol.String(c.info, "peer") {
		return epoch == protocol.String(c.info, "publisherEpoch")
	}
	r, ok := c.routes[peer]
	return ok && r.PublisherEpoch == epoch
}
func (c *Client) receivedRelayLocked(m Message) {
	peer := protocol.String(m, "peer")
	if r, ok := c.routes[peer]; ok {
		r.ReceivedRelay++
		c.routes[peer] = r
	}
}
func (c *Client) sentRelay(m Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	contains := func(raw any, peer string) bool {
		switch a := raw.(type) {
		case []string:
			for _, s := range a {
				if s == peer {
					return true
				}
			}
		case []any:
			for _, s := range a {
				if s == peer {
					return true
				}
			}
		}
		return false
	}
	to, hasTo := m["to"]
	exclude, hasExclude := m["exclude"]
	for peer, r := range c.routes {
		if hasTo && !contains(to, peer) || hasExclude && contains(exclude, peer) {
			continue
		}
		r.SentRelay++
		c.routes[peer] = r
	}
}
