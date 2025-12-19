package stream

import (
	"context"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type Handler struct {
	sharedQueue               workqueue.TypedRateLimitingInterface[StreamEvent]
	streamManager             StreamManager
	jobManager                JobManager
	logger                    klog.Logger
	streamRepository          StreamRepository
	backfillRequestRepository BackfillRequestRepository
}

type BackfillRequestRepository interface {
	CreateBackfillRequest(id StreamIdentity) error
}

func NewHandler(logger klog.Logger,
	provider QueueProvider,
	streamManager StreamManager,
	jobManager JobManager,
	streamRepository StreamRepository,
	backfillRequestRepository BackfillRequestRepository) *Handler {
	return &Handler{
		sharedQueue:               provider.GetQueue(),
		streamManager:             streamManager,
		jobManager:                jobManager,
		logger:                    logger,
		streamRepository:          streamRepository,
		backfillRequestRepository: backfillRequestRepository,
	}
}

func (s *Handler) Start(ctx context.Context) {
	go wait.UntilWithContext(ctx, func(_ context.Context) {

		s.HandleEvent()
	}, 0)
}

// HandleEvent processes a single event from the queue. It returns true if the queue is shutting down.
func (s *Handler) HandleEvent() bool {
	streamEvent, shutdown := s.sharedQueue.Get()

	if shutdown {
		return true
	}
	defer s.sharedQueue.Done(streamEvent)

	switch streamEvent.Type() {
	case Modified:
		s.handleModifiedEvent(streamEvent)
	case Added:
		s.handleAddedEvent(streamEvent)
	}

	return false
}

func (s *Handler) handleModifiedEvent(streamEvent StreamEvent) {
	if streamEvent.CrashLoopDetected() {
		s.logger.V(0).Info("Crash loop detected for the stream", "streamId", streamEvent.StreamId().String())
		err := s.streamManager.SetFailed(streamEvent)
		if err != nil {
			klog.V(0).ErrorS(err, "Failed to set Stream crash loop status", "streamId", streamEvent.StreamId().String())
		}
		return
	}

	if streamEvent.Suspended() {
		s.logger.V(0).Info("Stream suspended", "streamId", streamEvent.StreamId().String())
		err := s.jobManager.EnsureStopped(streamEvent.StreamId())
		if err != nil {
			s.logger.Error(err, "Failed to stop job for suspended stream", "streamId", streamEvent.StreamId().String())

			err = s.streamManager.SetFailed(streamEvent)
			if err != nil {
				s.logger.Error(err,
					"Failed to set Stream crash loop status for suspended stream",
					"streamId", streamEvent.StreamId().String())
			}
		}
		return
	}

}

func (s *Handler) handleAddedEvent(streamEvent StreamEvent) {
	if streamEvent.BackfillRequested() {
		s.logger.V(0).Info("Backfill requested for stream", "streamId", streamEvent.StreamId().String())
		err := s.jobManager.EnsureStopped(streamEvent.StreamId())
		if err != nil {
			s.logger.Error(err, "Failed to stop job for reload request", "streamId", streamEvent.StreamId().String())
		}

		streamDefinition, err := s.streamRepository.GetStreamById(streamEvent.StreamId())
		if err != nil {
			s.logger.V(0).Error(err, "Failed to get stream definition for backfill", "streamId", streamEvent.StreamId().String())
			return
		}

		err = s.jobManager.StartBackfill(streamDefinition)
		if err != nil {
			s.logger.V(0).Error(err, "Failed to start backfill job", "streamId", streamEvent.StreamId().String())

			err = s.streamManager.SetFailed(streamEvent)
			if err != nil {
				s.logger.Error(err,
					"Failed to set Stream job as failed after backfill job start failure",
					"streamId", streamEvent.StreamId().String())
			}
		}

		return
	}

	err := s.backfillRequestRepository.CreateBackfillRequest(streamEvent.StreamId())
	if err != nil {
		s.logger.V(0).Error(err, "Failed to create backfill request for new stream", "streamId", streamEvent.StreamId().String())

		err = s.streamManager.SetOperatorError(streamEvent)
		if err != nil {
			s.logger.V(0).Error(err, "Failed to set Stream operator error status", "streamId", streamEvent.StreamId().String())
		}
	}

}

func (s *Handler) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.V(0).Info("Shutting down stream handler")
			return
		default:
			completed := s.HandleEvent()
			if completed {
				return
			}
		}
	}
}
