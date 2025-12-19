package stream_class_tests

import (
	"context"
	"flag"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/generated/clientset/versioned"
	"github.com/SneaksAndData/arcane-operator/pkg/generated/informers/externalversions"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/tests"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_StreamClassWorkerIdentity(t *testing.T) {
	sc := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: uuid.New().String(),
		},
	}

	createdSc, err := versionedClientSet.StreamingV1().StreamClasses("").Create(t.Context(), sc, metav1.CreateOptions{})

	assert.NoError(t, err)
	assert.Equal(t, sc.WorkerId(), createdSc.WorkerId())
}

func Test_StreamClassCreationHandling(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: uuid.New().String(),
		},
	}

	controllerHandleMock := mocks.NewMockStreamControllerHandle(mockCtrl)
	factory := mocks.NewMockStreamControllerFactory(mockCtrl)

	factory.EXPECT().CreateStreamOperator(gomock.Any()).AnyTimes().Return(controllerHandleMock, nil)
	controllerHandleMock.EXPECT().Start().AnyTimes().Return(nil)

	controller, err := createController(t.Context(), factory)
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	_, err = versionedClientSet.StreamingV1().StreamClasses("").Create(t.Context(), sc, metav1.CreateOptions{})
	assert.NoError(t, err)
}

func Test_StreamClassDeletionHandling(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sc := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: uuid.New().String(),
		},
	}

	controllerHandleMock := mocks.NewMockStreamControllerHandle(mockCtrl)
	factory := mocks.NewMockStreamControllerFactory(mockCtrl)

	factory.EXPECT().CreateStreamOperator(gomock.Any()).AnyTimes().Return(controllerHandleMock, nil)

	waitChan := make(chan struct{})
	controllerHandleMock.EXPECT().Start().MinTimes(1).Return(nil)
	controllerHandleMock.EXPECT().IsUpdateNeeded(gomock.Any()).AnyTimes().Return(false)
	controllerHandleMock.EXPECT().Stop().MinTimes(1).Do(func() {
		waitChan <- struct{}{}
	})

	controller, err := createController(t.Context(), factory)
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	_, err = versionedClientSet.StreamingV1().StreamClasses("").Create(t.Context(), sc, metav1.CreateOptions{})
	assert.NoError(t, err)

	err = versionedClientSet.StreamingV1().StreamClasses("").Delete(t.Context(), sc.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	for {
		select {
		case <-t.Context().Done():
			t.Fatal("Test timed out waiting for StreamClass deletion handling")
		case <-waitChan:
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

var versionedClientSet versioned.Interface

func TestMain(m *testing.M) {
	err := flag.Set("test.timeout", "1m")
	if err != nil {
		panic(err)
	}

	flag.Parse()
	command := strings.Split(*tests.KubeconfigCmd, " ")

	output, err := tests.ReadExecKubeconfig(command)
	if err != nil {
		panic(err)
	}

	versionedClientSet = versioned.NewForConfigOrDie(output)
	err = clearStreamClasses(versionedClientSet)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

func createController(ctx context.Context, factory stream_class.StreamControllerFactory) (*stream_class.StreamClassController, error) {
	streamClassInformer := externalversions.NewSharedInformerFactory(versionedClientSet, 10*time.Minute).Streaming().V1().StreamClasses()
	go streamClassInformer.Informer().Run(ctx.Done())

	logger := klog.NewKlogr()
	worker := stream_class.NewStreamDefinitionControllerManager(logger, factory)
	handler := stream_class.NewStreamClassEventHandler(logger, conf.StreamClassOperatorConfiguration{}, worker)

	controller, err := stream_class.NewStreamClassController(streamClassInformer, handler)
	if err != nil {
		return nil, err
	}
	controller.Start(ctx)
	return controller, nil
}

func clearStreamClasses(client versioned.Interface) error {
	streams, err := client.StreamingV1().StreamClasses("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, stream := range streams.Items {
		err = client.StreamingV1().StreamClasses("").Delete(context.Background(), stream.Name, metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
