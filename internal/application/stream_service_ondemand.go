package application

import (
	"context"
	"fmt"
	"time"

	"hydrastream/internal/domain"
)

const defaultIdleStopDelay = 5 * time.Second

// AcquireStream registers an active consumer (mosaic, WHEP, AI) and starts upstream ingest on 0 -> 1 transition.
func (s *StreamService) AcquireStream(ctx context.Context, streamID, consumerID string) (int, error) {
	if streamID == "" {
		return 0, domain.ErrInvalidStream
	}
	if consumerID == "" {
		consumerID = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	st, err := s.repo.FindByID(ctx, streamID)
	if err != nil {
		return 0, err
	}

	s.consumerMu.Lock()
	if s.activeConsumers == nil {
		s.activeConsumers = make(map[string]map[string]struct{})
	}
	if s.idleTimers == nil {
		s.idleTimers = make(map[string]*time.Timer)
	}
	if t, ok := s.idleTimers[streamID]; ok && t != nil {
		t.Stop()
		delete(s.idleTimers, streamID)
	}
	set, exists := s.activeConsumers[streamID]
	if !exists {
		set = make(map[string]struct{})
		s.activeConsumers[streamID] = set
	}
	wasEmpty := len(set) == 0
	set[consumerID] = struct{}{}
	count := len(set)
	s.consumerMu.Unlock()

	if wasEmpty {
		if s.ingestor != nil {
			if err := s.ingestor.StartIngest(ctx, st); err != nil {
				return count, err
			}
		}
		go syncEngineStream(st.StreamID, st.SourceURL, st.Codec)
		s.RecordLog(domain.LogLevelInfo, "ondemand", fmt.Sprintf("Stream '%s' started on-demand (consumer=%s)", streamID, consumerID), nil)
	}

	return count, nil
}

// ReleaseStream unregisters a consumer and schedules upstream ingest stop when active consumers reach 0.
func (s *StreamService) ReleaseStream(_ context.Context, streamID, consumerID string) (int, error) {
	if streamID == "" {
		return 0, domain.ErrInvalidStream
	}

	s.consumerMu.Lock()
	set, exists := s.activeConsumers[streamID]
	if !exists {
		s.consumerMu.Unlock()
		return 0, nil
	}
	delete(set, consumerID)
	remaining := len(set)
	if remaining == 0 {
		delete(s.activeConsumers, streamID)
		delay := s.idleStopDelay
		if delay <= 0 {
			delay = defaultIdleStopDelay
		}
		s.idleTimers[streamID] = time.AfterFunc(delay, func() {
			s.stopIfIdle(streamID)
		})
	}
	s.consumerMu.Unlock()

	return remaining, nil
}

// ActiveConsumerCount returns the current number of fan-out consumers attached to a stream.
func (s *StreamService) ActiveConsumerCount(streamID string) int {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()
	return len(s.activeConsumers[streamID])
}

func (s *StreamService) stopIfIdle(streamID string) {
	s.consumerMu.Lock()
	count := len(s.activeConsumers[streamID])
	delete(s.idleTimers, streamID)
	s.consumerMu.Unlock()

	if count > 0 {
		return
	}
	if s.ingestor != nil {
		_ = s.ingestor.StopIngest(context.Background(), streamID)
	}
	go deleteEngineStream(streamID)
	s.RecordLog(domain.LogLevelInfo, "ondemand", fmt.Sprintf("Stream '%s' stopped after idle timeout (0 consumers)", streamID), nil)
}
