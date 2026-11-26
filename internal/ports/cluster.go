package ports

import (
	"context"

	"hydrastream/internal/domain"
)

// ClusterMesh defines the driven port for Master-Slave coordination, heartbeats, and state sync over NATS.
type ClusterMesh interface {
	PublishNodeHeartbeat(ctx context.Context, metrics domain.ClusterNodeMetrics) error
	SubscribeHeartbeats(handler func(domain.ClusterNodeMetrics)) error
	AssignStream(ctx context.Context, nodeID string, stream *domain.Stream) error
	UnassignStream(ctx context.Context, nodeID, streamID string) error
	SubscribeNodeCommands(
		nodeID string,
		onAssign func(context.Context, *domain.Stream) error,
		onUnassign func(context.Context, string) error,
	) error
	SaveClusterAssignment(ctx context.Context, assignment domain.StreamAssignment) error
	DeleteClusterAssignment(ctx context.Context, streamID string) error
	ListClusterAssignments(ctx context.Context) ([]domain.StreamAssignment, error)
	Close()
}
