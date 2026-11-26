package ingest

import "sync"

type rtpHub struct {
	mu   sync.RWMutex
	subs map[string]map[chan []byte]struct{}
}

func newRTPHub() *rtpHub {
	return &rtpHub{
		subs: make(map[string]map[chan []byte]struct{}),
	}
}

// SubscribeRTP registers a buffered non-blocking subscriber channel for a stream's raw RTP packets.
func (r *RTSPIngestor) SubscribeRTP(streamID string) (<-chan []byte, func()) {
	if r.hub == nil {
		ch := make(chan []byte)
		close(ch)
		return ch, func() {}
	}
	ch := make(chan []byte, 128)
	r.hub.mu.Lock()
	if r.hub.subs[streamID] == nil {
		r.hub.subs[streamID] = make(map[chan []byte]struct{})
	}
	r.hub.subs[streamID][ch] = struct{}{}
	r.hub.mu.Unlock()

	cancel := func() {
		r.hub.mu.Lock()
		if set := r.hub.subs[streamID]; set != nil {
			if _, ok := set[ch]; ok {
				delete(set, ch)
				close(ch)
			}
			if len(set) == 0 {
				delete(r.hub.subs, streamID)
			}
		}
		r.hub.mu.Unlock()
	}
	return ch, cancel
}

func (h *rtpHub) publish(streamID string, rtpPacket []byte) {
	if h == nil || len(rtpPacket) == 0 {
		return
	}
	h.mu.RLock()
	set := h.subs[streamID]
	if len(set) == 0 {
		h.mu.RUnlock()
		return
	}
	pktCopy := make([]byte, len(rtpPacket))
	copy(pktCopy, rtpPacket)
	for ch := range set {
		select {
		case ch <- pktCopy:
		default:
			// Drop packet for slow consumer rather than stalling RTSP ingest
		}
	}
	h.mu.RUnlock()
}
