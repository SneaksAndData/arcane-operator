package tests

import (
	"flag"
	"fmt"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os/exec"
)

var KubeconfigCmd = flag.String("cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to get kubeconfig")

func ReadExecKubeconfig(command []string) (*rest.Config, error) {
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
