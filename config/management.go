package config

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"k8s.io/klog/v2"
	"os"
	"strings"
)

const (
	EnvPrefix = "ARCANE_OPERATOR_" // varnames will be NEXUS__MY_ENV_VAR or NEXUS__SECTION1__SECTION2__MY_ENV_VAR
)

func configExists(configPath string) (bool, error) {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil { // coverage-ignore
		return false, err
	} else {
		return true, nil
	}
}

func LoadConfig[T any](ctx context.Context) (*T, error) {
	logger := klog.FromContext(ctx)
	customViper := viper.NewWithOptions(viper.KeyDelimiter("__"))
	customViper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	localConfig := fmt.Sprintf("appconfig.%s.yaml", strings.ToLower(os.Getenv("APPLICATION_ENVIRONMENT")))

	exists, err := configExists(localConfig)
	if err != nil {
		return nil, fmt.Errorf("error checking config for existance: %w", err)
	}

	if exists {
		customViper.SetConfigFile(fmt.Sprintf("appconfig.%s.yaml", strings.ToLower(os.Getenv("APPLICATION_ENVIRONMENT"))))
	} else {
		logger.Info("no environment specific config found, loading default appconfig.yaml")
		customViper.SetConfigFile("appconfig.yaml")
	}

	customViper.SetEnvPrefix(EnvPrefix)
	customViper.AllowEmptyEnv(true)
	customViper.AutomaticEnv()

	err = customViper.ReadInConfig()
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var appConfig T
	err = customViper.Unmarshal(&appConfig)

	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	return &appConfig, nil
}
