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
	s := &ProbesService{
		addr: probesConfig.Addr,
		srv: &http.Server{
			Addr:         probesConfig.Addr,
			WriteTimeout: probesConfig.WriteTimeout,
			ReadTimeout:  probesConfig.ReadTimeout,
		},
		shutdownTimeout: probesConfig.ShutdownTimeout,
	}

	http.HandleFunc("/startup", s.startupProbe)
	http.HandleFunc("/health", s.livenessProbe)
	http.HandleFunc("/health/ready", s.readinessProbe)

	return s
}

func (s *ProbesService) livenessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *ProbesService) readinessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *ProbesService) startupProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("OK"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
