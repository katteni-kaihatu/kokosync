package core

import (
	"context"
	"time"
)

// RunRoomCleanup releases expired empty rooms even when no clients are joining.
// Stop it by cancelling ctx when the server shuts down.
func (c *Core) RunRoomCleanup(ctx context.Context) {
	interval := min(time.Minute, time.Duration(c.Config.RoomIdleTimeoutMs)*time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			c.mu.Lock()
			c.pruneIdleRoomsLocked(now)
			c.mu.Unlock()
		}
	}
}

func (c *Core) pruneIdleRoomsLocked(now time.Time) {
	timeout := time.Duration(c.Config.RoomIdleTimeoutMs) * time.Millisecond
	for name, r := range c.rooms {
		if len(r.peers) != 0 || r.idleSince.IsZero() || now.Sub(r.idleSince) < timeout {
			continue
		}
		c.stored -= r.historyBytes + r.stateBytes + r.dedupBytes + r.checkpointBytes
		delete(c.rooms, name)
	}
}

type RoomStats struct {
	Count         int `json:"count"`
	Active        int `json:"active"`
	Empty         int `json:"empty"`
	Peers         int `json:"peers"`
	Max           int `json:"max"`
	IdleTimeoutMs int `json:"idleTimeoutMs"`
	// StoredBytes counts encoded retained data, not process memory usage.
	StoredBytes int `json:"storedBytes"`
}

func (c *Core) RoomStats() RoomStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := RoomStats{Count: len(c.rooms), Max: c.Config.MaxRooms, IdleTimeoutMs: c.Config.RoomIdleTimeoutMs, StoredBytes: c.stored}
	for _, r := range c.rooms {
		out.Peers += len(r.peers)
		if len(r.peers) == 0 {
			out.Empty++
		} else {
			out.Active++
		}
	}
	return out
}
