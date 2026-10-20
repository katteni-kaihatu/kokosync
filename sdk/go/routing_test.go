package kokosync

import "testing"

func TestRelayRoutesTrackMembershipEpochAndCounters(t *testing.T) {
	c := &Client{}
	c.trackRoutingLocked(Message{"type": "joined", "peer": "local", "epoch": "room", "publisherEpoch": "local-1", "capabilities": Message{"routeSignaling": true}, "peers": []any{Message{"peer": "other", "publisherEpoch": "other-1"}}})
	if routes := c.Routes(); len(routes) != 1 || routes[0].Route != "relay" || routes[0].Reason != "local-adapter-unavailable" || routes[0].RTTMs != nil {
		t.Fatal("native route misrepresented")
	}
	if c.trackRoutingLocked(Message{"type": "peer_joined", "epoch": "room", "peer": "other", "publisherEpoch": "stale"}) != nil {
		t.Fatal("stale generation overwrote member")
	}
	c.sentRelay(Message{})
	c.receivedRelayLocked(Message{"peer": "other"})
	r := c.Routes()[0]
	if r.SentRelay != 1 || r.ReceivedRelay != 1 || r.SentDirect != 0 {
		t.Fatal("route counters incorrect")
	}
	if c.validRealtimeLocked(Message{"peer": "other", "publisherEpoch": "stale"}) {
		t.Fatal("stale realtime accepted")
	}
	if c.trackRoutingLocked(Message{"type": "peer_left", "epoch": "old-room", "peer": "other", "publisherEpoch": "other-1"}) != nil {
		t.Fatal("stale room departure accepted")
	}
	if c.trackRoutingLocked(Message{"type": "peer_left", "epoch": "room", "peer": "other", "publisherEpoch": "other-1"}) == nil || len(c.Routes()) != 0 {
		t.Fatal("member was not removed")
	}
}
