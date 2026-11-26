package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

const (
	subjectHeartbeatWildcard = "hydra.v1.cluster.heartbeat.*"
	clusterKVBucket          = "hydra_cluster_streams"
)

type commandAck struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// ClusterNATSMesh implements ports.ClusterMesh over NATS Core + optional JetStream KV.
type ClusterNATSMesh struct {
	nc   *nats.Conn
	kv   nats.KeyValue
	mu   sync.Mutex
	subs []*nats.Subscription
}

// NewClusterNATSMesh wraps an existing NATS connection and initializes JetStream KV if available.
func NewClusterNATSMesh(nc *nats.Conn) *ClusterNATSMesh {
	mesh := &ClusterNATSMesh{nc: nc}
	if nc == nil {
		return mesh
	}

	if js, err := nc.JetStream(); err == nil {
		kv, err := js.KeyValue(clusterKVBucket)
		if err != nil {
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:  clusterKVBucket,
				History: 1,
			})
		}
		if err == nil {
			mesh.kv = kv
			log.Printf("🗄️ [ClusterMesh] JetStream KV bucket '%s' ready for distributed stream state\n", clusterKVBucket)
		}
	}
	return mesh
}

func (m *ClusterNATSMesh) PublishNodeHeartbeat(_ context.Context, metrics domain.ClusterNodeMetrics) error {
	if m.nc == nil {
		return nil
	}
	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("hydra.v1.cluster.heartbeat.%s", metrics.NodeID)
	return m.nc.Publish(subject, data)
}

func (m *ClusterNATSMesh) SubscribeHeartbeats(handler func(domain.ClusterNodeMetrics)) error {
	if m.nc == nil {
		return nil
	}
	sub, err := m.nc.Subscribe(subjectHeartbeatWildcard, func(msg *nats.Msg) {
		var node domain.ClusterNodeMetrics
		if err := json.Unmarshal(msg.Data, &node); err == nil {
			handler(node)
		}
	})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.subs = append(m.subs, sub)
	m.mu.Unlock()
	return nil
}

func (m *ClusterNATSMesh) AssignStream(ctx context.Context, nodeID string, stream *domain.Stream) error {
	if m.nc == nil {
		return errors.New("nats connection unavailable")
	}
	payload, err := json.Marshal(stream)
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("hydra.v1.cluster.node.%s.assign", nodeID)
	timeout := 6 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}
	msg, err := m.nc.Request(subject, payload, timeout)
	if err != nil {
		return fmt.Errorf("assign request to node %s failed: %w", nodeID, err)
	}
	var ack commandAck
	if err := json.Unmarshal(msg.Data, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return errors.New(ack.Error)
	}
	return nil
}

func (m *ClusterNATSMesh) UnassignStream(ctx context.Context, nodeID, streamID string) error {
	if m.nc == nil {
		return errors.New("nats connection unavailable")
	}
	payload, err := json.Marshal(map[string]string{"stream_id": streamID})
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("hydra.v1.cluster.node.%s.unassign", nodeID)
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}
	msg, err := m.nc.Request(subject, payload, timeout)
	if err != nil {
		return fmt.Errorf("unassign request to node %s failed: %w", nodeID, err)
	}
	var ack commandAck
	if err := json.Unmarshal(msg.Data, &ack); err != nil {
		return err
	}
	if !ack.OK {
		return errors.New(ack.Error)
	}
	return nil
}

func (m *ClusterNATSMesh) SubscribeNodeCommands(
	nodeID string,
	onAssign func(context.Context, *domain.Stream) error,
	onUnassign func(context.Context, string) error,
) error {
	if m.nc == nil {
		return nil
	}

	assignSubj := fmt.Sprintf("hydra.v1.cluster.node.%s.assign", nodeID)
	s1, err := m.nc.Subscribe(assignSubj, func(msg *nats.Msg) {
		var st domain.Stream
		ack := commandAck{OK: true}
		if err := json.Unmarshal(msg.Data, &st); err != nil {
			ack = commandAck{OK: false, Error: err.Error()}
		} else if err := onAssign(context.Background(), &st); err != nil {
			ack = commandAck{OK: false, Error: err.Error()}
		}
		reply, _ := json.Marshal(ack)
		_ = msg.Respond(reply)
	})
	if err != nil {
		return err
	}

	unassignSubj := fmt.Sprintf("hydra.v1.cluster.node.%s.unassign", nodeID)
	s2, err := m.nc.Subscribe(unassignSubj, func(msg *nats.Msg) {
		var req struct {
			StreamID string `json:"stream_id"`
		}
		ack := commandAck{OK: true}
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			ack = commandAck{OK: false, Error: err.Error()}
		} else if err := onUnassign(context.Background(), req.StreamID); err != nil {
			ack = commandAck{OK: false, Error: err.Error()}
		}
		reply, _ := json.Marshal(ack)
		_ = msg.Respond(reply)
	})
	if err != nil {
		_ = s1.Unsubscribe()
		return err
	}

	m.mu.Lock()
	m.subs = append(m.subs, s1, s2)
	m.mu.Unlock()
	return nil
}

func (m *ClusterNATSMesh) SaveClusterAssignment(_ context.Context, assignment domain.StreamAssignment) error {
	if m.kv == nil {
		return nil
	}
	data, err := json.Marshal(assignment)
	if err != nil {
		return err
	}
	_, err = m.kv.Put(assignment.StreamID, data)
	return err
}

func (m *ClusterNATSMesh) DeleteClusterAssignment(_ context.Context, streamID string) error {
	if m.kv == nil {
		return nil
	}
	err := m.kv.Delete(streamID)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (m *ClusterNATSMesh) ListClusterAssignments(_ context.Context) ([]domain.StreamAssignment, error) {
	if m.kv == nil {
		return nil, nil
	}
	keys, err := m.kv.Keys()
	if err != nil {
		if errors.Is(err, nats.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	var out []domain.StreamAssignment
	for _, k := range keys {
		entry, err := m.kv.Get(k)
		if err != nil {
			continue
		}
		var a domain.StreamAssignment
		if err := json.Unmarshal(entry.Value(), &a); err == nil {
			out = append(out, a)
		}
	}
	return out, nil
}

func (m *ClusterNATSMesh) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.subs {
		_ = s.Unsubscribe()
	}
	m.subs = nil
}

var _ ports.ClusterMesh = (*ClusterNATSMesh)(nil)
