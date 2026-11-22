package flowmanagerextension

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/go-logr/zapr"
	flowv1 "github.com/grepplabs/jetstream-collector/api/telemetryflow/v1alpha1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	k8scache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const componentType = "flowmanager"

type flowManagerExtension struct {
	cfg                *Config
	logger             *zap.Logger
	telemetry          component.TelemetrySettings
	buildInfo          component.BuildInfo
	factories          factories
	host               component.Host
	restConfigOverride *rest.Config
	kubeCache          cache.Cache
	flowSelector       labels.Selector
	reconcileCh        chan struct{}
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	shutdown           sync.Once
	active             atomic.Pointer[runtimeGraph]
	debugServer        *http.Server
}

var _ extension.Extension = (*flowManagerExtension)(nil)

func NewFactory() extension.Factory {
	typ := component.MustNewType(componentType)
	return extension.NewFactory(typ,
		func() component.Config { return NewDefaultConfig() },
		func(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
			c, ok := cfg.(*Config)
			if !ok {
				return nil, fmt.Errorf("config structure is not *flowmanagerextension.Config")
			}
			if err := c.Validate(); err != nil {
				return nil, err
			}
			logger := set.Logger
			if logger == nil {
				logger = zap.NewNop()
			}
			return &flowManagerExtension{cfg: c, logger: logger, telemetry: set.TelemetrySettings, buildInfo: set.BuildInfo, factories: defaultFactories(), reconcileCh: make(chan struct{}, 1)}, nil
		}, component.StabilityLevelDevelopment)
}

func (e *flowManagerExtension) Start(ctx context.Context, host component.Host) error {
	e.host = host
	ctrllog.SetLogger(zapr.NewLogger(e.logger))
	cfg, err := e.restConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	labelSelector := e.cfg.Selector.labelSelector()
	selector, err := metav1.LabelSelectorAsSelector(&labelSelector)
	if err != nil {
		return fmt.Errorf("build TelemetryFlow label selector: %w", err)
	}
	e.flowSelector = selector
	scheme := runtime.NewScheme()
	if err := flowv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register TelemetryFlow API types: %w", err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{ByObject: map[ctrlclient.Object]cache.ByObject{
			&flowv1.TelemetryFlow{}: {
				Namespaces: map[string]cache.Config{e.cfg.Namespace: {}},
				Label:      selector,
			},
		}, ReaderFailOnMissingInformer: true},
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
	if err != nil {
		return fmt.Errorf("create controller-runtime manager: %w", err)
	}
	informer, err := mgr.GetCache().GetInformer(ctx, &flowv1.TelemetryFlow{})
	if err != nil {
		return fmt.Errorf("create TelemetryFlow informer: %w", err)
	}
	e.kubeCache = mgr.GetCache()
	_, err = informer.AddEventHandler(k8scache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { e.signalReconcile() },
		UpdateFunc: func(_, _ any) { e.signalReconcile() },
		DeleteFunc: func(any) { e.signalReconcile() },
	})
	if err != nil {
		return fmt.Errorf("register TelemetryFlow event handler: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	managerDone := make(chan error, 1)
	e.wg.Go(func() {
		err := mgr.Start(runCtx)
		managerDone <- err
		if err != nil && runCtx.Err() == nil {
			e.logger.Error("Controller-runtime manager stopped unexpectedly", zap.Error(err))
			cancel()
		}
	})
	cacheSynced := make(chan bool, 1)
	e.wg.Go(func() { cacheSynced <- mgr.GetCache().WaitForCacheSync(runCtx) })
	select {
	case synced := <-cacheSynced:
		if !synced {
			cancel()
			return fmt.Errorf("TelemetryFlow controller-runtime cache failed to sync")
		}
	case err := <-managerDone:
		cancel()
		if err == nil {
			return fmt.Errorf("controller-runtime manager stopped before cache sync")
		}
		return fmt.Errorf("controller-runtime manager stopped before cache sync: %w", err)
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
	if err := e.reconcile(runCtx); err != nil {
		e.logger.Error("Initial TelemetryFlow graph was rejected", zap.Error(err))
	}
	e.wg.Go(func() { e.reconcileLoop(runCtx) })
	if err := e.startDebugEndpoint(); err != nil {
		cancel()
		e.wg.Wait()
		if graph := e.active.Swap(nil); graph != nil {
			_ = graph.shutdown(context.Background())
		}
		return err
	}
	e.logger.Info("TelemetryFlow watch synchronized", zap.String("namespace", e.cfg.Namespace))
	return nil
}

func (e *flowManagerExtension) Shutdown(ctx context.Context) error {
	var shutdownErr error
	e.shutdown.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}
		if e.debugServer != nil {
			if err := e.debugServer.Shutdown(ctx); err != nil {
				shutdownErr = fmt.Errorf("shut down config dump endpoint: %w", err)
				_ = e.debugServer.Close()
			}
		}
		e.wg.Wait()
		if graph := e.active.Swap(nil); graph != nil {
			shutdownErr = errors.Join(shutdownErr, graph.shutdown(ctx))
		}
	})
	return shutdownErr
}

func (e *flowManagerExtension) listFlows(ctx context.Context) ([]*flowv1.TelemetryFlow, error) {
	var list flowv1.TelemetryFlowList
	if err := e.kubeCache.List(ctx, &list,
		ctrlclient.InNamespace(e.cfg.Namespace),
		ctrlclient.MatchingLabelsSelector{Selector: e.flowSelector},
	); err != nil {
		return nil, fmt.Errorf("list TelemetryFlows from informer cache: %w", err)
	}
	flows := make([]*flowv1.TelemetryFlow, 0, len(list.Items))
	for i := range list.Items {
		if !list.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		flows = append(flows, list.Items[i].DeepCopy())
	}
	return flows, nil
}

func (e *flowManagerExtension) signalReconcile() {
	select {
	case e.reconcileCh <- struct{}{}:
	default:
	}
}

func (e *flowManagerExtension) restConfig() (*rest.Config, error) {
	if e.restConfigOverride != nil {
		return e.restConfigOverride, nil
	}
	if e.cfg.Kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", e.cfg.Kubeconfig)
}
