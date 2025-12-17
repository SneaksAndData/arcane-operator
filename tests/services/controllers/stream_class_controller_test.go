package controllers

import (
	"flag"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/generated/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/clientcmd"
)

func Test_StreamClassWorkerIdentity(t *testing.T) {
	sc := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
	}

	createdSc, err := clientSet.StreamingV1().StreamClasses("").Create(t.Context(), sc, metav1.CreateOptions{})

	if err != nil {
		t.Fatalf("Failed to create StreamClass: %v", err)
	}
	assert.Equal(t, sc.WorkerId(), createdSc.WorkerId())
}

var cmd = flag.String("cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to get kubeconfig")
var clientSet *versioned.Clientset

func TestMain(m *testing.M) {
	flag.Parse()
	command := strings.Split(*cmd, " ")

	output, err := readExecKubeconfig(command)
	if err != nil {
		panic(err)
	}

	clientSet = versioned.NewForConfigOrDie(output)
	code := m.Run()
	os.Exit(code)
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
