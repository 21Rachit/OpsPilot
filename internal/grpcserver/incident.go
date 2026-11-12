package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	incidentv1 "opspilot/gen/incident/v1"
	"opspilot/internal/model"
	"opspilot/internal/store"
)

// Store is what the gRPC server needs from storage. It's the same set of methods as the
// HTTP handler's IncidentStore, so the same store (Postgres + cache) serves both APIs.
type Store interface {
	Create(ctx context.Context, title, severity string) (model.Incident, error)
	Get(ctx context.Context, id int) (model.Incident, error)
	List(ctx context.Context) ([]model.Incident, error)
	Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error)
	Delete(ctx context.Context, id int) error
}

// IncidentServer implements the IncidentService defined in proto/incident/v1/incident.proto.
type IncidentServer struct {
	// Embedding this is required by the generated code. If you add an RPC to the .proto
	// and regenerate, the new method returns codes.Unimplemented until you write it,
	// instead of the build breaking.
	incidentv1.UnimplementedIncidentServiceServer
	store Store
}

func NewIncidentServer(s Store) *IncidentServer {
	return &IncidentServer{store: s}
}

func (s *IncidentServer) CreateIncident(ctx context.Context, req *incidentv1.CreateIncidentRequest) (*incidentv1.Incident, error) {
	if err := model.Validate(req.GetTitle(), req.GetSeverity(), "", false); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	inc, err := s.store.Create(ctx, req.GetTitle(), req.GetSeverity())
	if err != nil {
		return nil, storeError(err)
	}
	slog.Info("incident created", "id", inc.ID, "severity", inc.Severity, "api", "grpc")
	return toProto(inc), nil
}

func (s *IncidentServer) GetIncident(ctx context.Context, req *incidentv1.GetIncidentRequest) (*incidentv1.Incident, error) {
	if req.GetId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be a positive number")
	}
	inc, err := s.store.Get(ctx, int(req.GetId()))
	if err != nil {
		return nil, storeError(err)
	}
	return toProto(inc), nil
}

func (s *IncidentServer) ListIncidents(ctx context.Context, _ *incidentv1.ListIncidentsRequest) (*incidentv1.ListIncidentsResponse, error) {
	list, err := s.store.List(ctx)
	if err != nil {
		return nil, storeError(err)
	}
	resp := &incidentv1.ListIncidentsResponse{Incidents: make([]*incidentv1.Incident, 0, len(list))}
	for _, inc := range list {
		resp.Incidents = append(resp.Incidents, toProto(inc))
	}
	return resp, nil
}

func (s *IncidentServer) UpdateIncident(ctx context.Context, req *incidentv1.UpdateIncidentRequest) (*incidentv1.Incident, error) {
	if req.GetId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be a positive number")
	}
	if err := model.Validate(req.GetTitle(), req.GetSeverity(), req.GetStatus(), true); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	inc, err := s.store.Update(ctx, int(req.GetId()), req.GetTitle(), req.GetSeverity(), req.GetStatus())
	if err != nil {
		return nil, storeError(err)
	}
	slog.Info("incident updated", "id", inc.ID, "status", inc.Status, "severity", inc.Severity, "api", "grpc")
	return toProto(inc), nil
}

func (s *IncidentServer) DeleteIncident(ctx context.Context, req *incidentv1.DeleteIncidentRequest) (*incidentv1.DeleteIncidentResponse, error) {
	if req.GetId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be a positive number")
	}
	if err := s.store.Delete(ctx, int(req.GetId())); err != nil {
		return nil, storeError(err)
	}
	slog.Info("incident deleted", "id", req.GetId(), "api", "grpc")
	return &incidentv1.DeleteIncidentResponse{}, nil
}

// toProto converts our model to the generated protobuf type.
func toProto(inc model.Incident) *incidentv1.Incident {
	return &incidentv1.Incident{
		Id:        int64(inc.ID),
		Title:     inc.Title,
		Severity:  inc.Severity,
		Status:    inc.Status,
		CreatedAt: timestamppb.New(inc.CreatedAt),
	}
}

// storeError turns a store error into a gRPC status, like storeError does for HTTP.
// The details of unexpected errors are logged, not sent to the caller.
func storeError(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	slog.Error("unexpected store error", "error", err, "api", "grpc")
	return status.Error(codes.Internal, "internal error")
}
