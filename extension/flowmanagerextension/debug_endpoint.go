package flowmanagerextension

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
	"sigs.k8s.io/yaml"
)

const configDumpPath = "/debug/flowmanager/config"

func (e *flowManagerExtension) startDebugEndpoint() error {
	if e.cfg.DebugEndpoint == "" {
		return nil
	}

	listener, err := net.Listen("tcp", e.cfg.DebugEndpoint)
	if err != nil {
		return fmt.Errorf("listen on debug endpoint %q: %w", e.cfg.DebugEndpoint, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+configDumpPath, e.handleConfigDump)
	server := &http.Server{
		Addr:              e.cfg.DebugEndpoint,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	e.debugServer = server
	e.wg.Go(func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.logger.Error("Config dump endpoint stopped unexpectedly", zap.Error(err))
		}
	})
	return nil
}

func (e *flowManagerExtension) handleConfigDump(w http.ResponseWriter, _ *http.Request) {
	graph := e.active.Load()
	if graph == nil {
		http.Error(w, "no active flow graph", http.StatusServiceUnavailable)
		return
	}

	data, err := yaml.Marshal(graph.config)
	if err != nil {
		http.Error(w, "could not encode active flow graph as YAML", http.StatusInternalServerError)
		e.logger.Error("Could not encode active TelemetryFlow graph as YAML", zap.Error(err))
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}
