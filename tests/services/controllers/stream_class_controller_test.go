package controllers

import (
	"context"
	"flag"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/generated/clientset/versioned"
	"github.com/SneaksAndData/arcane-operator/pkg/generated/informers/externalversions"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
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

func Test_StreamClassInformer(t *testing.T) {
	sc := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: uuid.New().String(),
		},
	}

	controller, err := createController(t.Context(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	_, err = versionedClientSet.StreamingV1().StreamClasses("").Create(t.Context(), sc, metav1.CreateOptions{})
	assert.NoError(t, err)
	time.Sleep(15 * time.Second)
	//assert.Equal(t, sc.WorkerId(), createdSc.WorkerId())
}

var cmd = flag.String("cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to get kubeconfig")
var versionedClientSet versioned.Interface

func TestMain(m *testing.M) {
	err := flag.Set("test.timeout", "1m")
	if err != nil {
		panic(err)
	}

	flag.Parse()
	command := strings.Split(*cmd, " ")

	output, err := readExecKubeconfig(command)
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

func readExecKubeconfig(command []string) (*rest.Config, error) {
	proc := exec.Command(command[0], command[1:]...)
	output, err := proc.Output()
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to execute command %s: %w", command, err)
	}

	cfg, err := clientcmd.RESTConfigFromKubeConfig(output)
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}
	return cfg, nil
}
