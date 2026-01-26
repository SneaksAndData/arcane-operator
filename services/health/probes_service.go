package health

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// ProbesService provides HTTP endpoints for health probes.
type ProbesService struct {

	// addr is the address the health probes server listens on.
	addr string

	// srv is the HTTP server for health probes.
	srv *http.Server

	// shutdownTimeout is the maximum duration for the health probes server to shut down gracefully.
	shutdownTimeout time.Duration
}

func NewProbesService(probesConfig ProbesConfig) *ProbesService {
	mux := http.NewServeMux()

	s := &ProbesService{
		addr:            probesConfig.Addr,
		shutdownTimeout: probesConfig.ShutdownTimeout,
	}

	s.srv = &http.Server{
		Addr:         probesConfig.Addr,
		WriteTimeout: probesConfig.WriteTimeout,
		ReadTimeout:  probesConfig.ReadTimeout,
		Handler:      mux,
	}

	mux.HandleFunc("/startup", s.startupProbe)
	mux.HandleFunc("/health", s.livenessProbe)
	mux.HandleFunc("/health/ready", s.readinessProbe)
	return s
}

func (s *ProbesService) livenessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		panic(err)
	}
}

func (s *ProbesService) readinessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		panic(err)
	}
}

func (s *ProbesService) startupProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		panic(err)
	}
}

func (s *ProbesService) ListenAndServe(ctx context.Context) error {
	errChan := make(chan error, 1)

	go func() {
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()

		err := s.srv.Shutdown(shutdownCtx)
		if err != nil {
			return err
		}
		return ctx.Err()
	}
}
