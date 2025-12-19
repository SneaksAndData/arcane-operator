package stream_tests

import (
	"flag"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/tests"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"os"
	"strings"
	"testing"
)

func Test_StreamAdded(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	qp := NewQueueProvider()
	q := qp.GetQueue()

	jobManager := mocks.NewMockJobManager(mockCtrl)

	// JobManager should ensure that any existing jobs are stopped before starting a backfill
	jobManager.EXPECT().EnsureStopped(gomock.Any()).AnyTimes().Return(nil)
	jobManager.EXPECT().StartBackfill(gomock.Any())

	repository := mocks.NewMockStreamRepository(mockCtrl)

	// Mock the repository to return a mock stream definition object
	repository.EXPECT().GetStreamById(gomock.Any()).AnyTimes().Return(nil, nil)

	streamManager := mocks.NewMockStreamManager(mockCtrl)
	backfillRequestRepository := mocks.NewMockBackfillRequestRepository(mockCtrl)

	// Simulate the real behavior: when a stream was added, the backfill request should be created
	// And then the backfill request event should be published to the queue
	backfillRequestRepository.EXPECT().CreateBackfillRequest(gomock.Any()).Times(1).Do(func(obj any) {
		q.Add(AddedBackfillRequest(mockCtrl))
		q.ShutDown()
	})

	handler := createHandler(qp, streamManager, jobManager, repository, backfillRequestRepository)
	assert.NotNil(t, handler)

	q.Add(AddedStreamEvent(mockCtrl))

	// Run the handler to process the events
	handler.Run(t.Context())
}

func AddedStreamEvent(mockCtrl *gomock.Controller) stream.StreamEvent {
	ev := mocks.NewMockStreamEvent(mockCtrl)
	ev.EXPECT().BackfillRequested().Return(false).Times(1)
	ev.EXPECT().Type().Return(stream.Added).AnyTimes()
	ev.EXPECT().StreamId().AnyTimes().Return(stream.StreamIdentity{Namespace: "namespace", StreamId: "stream-name"})
	return ev
}

func AddedBackfillRequest(mockCtrl *gomock.Controller) stream.StreamEvent {
	ev := mocks.NewMockStreamEvent(mockCtrl)
	ev.EXPECT().BackfillRequested().Return(true).Times(1)
	ev.EXPECT().Type().Return(stream.Added).AnyTimes()
	ev.EXPECT().StreamId().AnyTimes().Return(stream.StreamIdentity{Namespace: "namespace", StreamId: "stream-name"})
	return ev
}

func TestMain(m *testing.M) {
	err := flag.Set("test.timeout", "1m")
	if err != nil {
		panic(err)
	}

	flag.Parse()
	_ = strings.Split(*tests.KubeconfigCmd, " ")

	code := m.Run()
	os.Exit(code)
}

func createHandler(queueProvider QueueProvider, streamManager stream.StreamManager,
	jobManager stream.JobManager,
	repository stream.StreamRepository,
	requestRepository stream.BackfillRequestRepository) *stream.Handler {

	return stream.NewHandler(klog.NewKlogr(), queueProvider, streamManager, jobManager, repository, requestRepository)
}

type QueueProvider struct {
	queue workqueue.TypedRateLimitingInterface[stream.StreamEvent]
}

func (s QueueProvider) GetQueue() workqueue.TypedRateLimitingInterface[stream.StreamEvent] {
	return s.queue
}

func NewQueueProvider() QueueProvider {
	rateLimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[stream.StreamEvent](0, 0),
		&workqueue.TypedBucketRateLimiter[stream.StreamEvent]{
			Limiter: rate.NewLimiter(0, 0),
		},
	)
	return QueueProvider{
		queue: workqueue.NewTypedRateLimitingQueue[stream.StreamEvent](rateLimiter),
	}
}
