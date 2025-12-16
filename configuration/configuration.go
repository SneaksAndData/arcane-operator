package configuration

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/spf13/viper"
	"k8s.io/klog/v2"
	"os"
	"strings"
)

const (
	EnvPrefix = "ARCANE_OPERATOR_"
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

func LoadConfig(ctx context.Context) conf.ApplicationConfiguration {
	logger := klog.FromContext(ctx)
	customViper := viper.NewWithOptions(viper.KeyDelimiter("__"))
	customViper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	localConfig := fmt.Sprintf("appconfig.%s.yaml", strings.ToLower(os.Getenv("APPLICATION_ENVIRONMENT")))

	if exists, err := configExists(localConfig); exists {
		customViper.SetConfigFile(fmt.Sprintf("appconfig.%s.yaml", strings.ToLower(os.Getenv("APPLICATION_ENVIRONMENT"))))
	} else if err != nil { // coverage-ignore
		logger.Error(err, "could not locate application configuration file")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	} else {
		customViper.SetConfigFile("appconfig.yaml")
	}

	customViper.SetEnvPrefix(EnvPrefix)
	customViper.AllowEmptyEnv(true)
	customViper.AutomaticEnv()

	if err := customViper.ReadInConfig(); err != nil { // coverage-ignore
		logger.Error(err, "error loading application config from appconfig.yaml")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	var appConfig conf.ApplicationConfiguration
	err := customViper.Unmarshal(&appConfig)

	if err != nil { // coverage-ignore
		logger.Error(err, "error loading application config from appconfig.yaml")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	return appConfig
}
