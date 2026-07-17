package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/agent"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/config"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/failover"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/httpserver"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("config_invalid", "error", err)
		os.Exit(1)
	}
	if warning := cfg.LegacyAdminLoginWarning(); warning != "" {
		logger.Warn("legacy_admin_key_login_enabled", "warning", warning)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0o750); err != nil {
		logger.Error("data_directory_failed", "error", err)
		os.Exit(1)
	}
	database, err := store.Open(cfg.DatabasePath, model.Settings{
		DryRun: cfg.InitialDryRun, FailureThreshold: cfg.FailureThreshold,
		RecoveryThreshold: cfg.RecoveryThreshold, ManualHoldMinutes: cfg.ManualHoldMinutes,
		FlapWindowMinutes: cfg.FlapWindowMinutes, FlapPauseThreshold: cfg.FlapPauseThreshold,
		FlapRecoveryThreshold: cfg.FlapRecoveryThreshold,
	})
	if err != nil {
		logger.Error("database_open_failed", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	client := sub2api.New(cfg.Sub2APIBaseURL, cfg.AdminAPIKey, cfg.RequestTimeout)
	engine := reconcile.NewEngine(client, database, cfg.PollInterval, logger)
	var secretBox *balance.SecretBox
	if len(cfg.CredentialKey) > 0 {
		secretBox, err = balance.NewSecretBox(cfg.CredentialKey)
		if err != nil {
			logger.Error("credential_key_invalid", "error", err)
			os.Exit(1)
		}
	}
	if sourceCount, countErr := database.CountUpstreamSources(context.Background()); countErr != nil {
		logger.Error("upstream_count_failed", "error", countErr)
		os.Exit(1)
	} else if sourceCount > 0 && secretBox == nil {
		logger.Error("credential_key_required", "error", "UPSTREAM_CREDENTIAL_KEY is required when upstream sources exist")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	engine.Start(ctx)
	telemetryManager := telemetry.NewManager(client, database, cfg.TelemetryPollInterval, logger,
		telemetry.WithReconcileRequester(engine), telemetry.WithMonitorAccountResolver(engine))
	balanceManager := balance.NewManager(database, client, engine, balance.NewFetcher(cfg.RequestTimeout, cfg.AllowInsecureUpstream), secretBox, cfg.BalancePollInterval, logger)
	if err := balanceManager.RecoverGroupTransitions(ctx); err != nil {
		logger.Warn("group_transition_recovery_incomplete", "error", err)
	}
	balanceManager.Start(ctx)
	failoverController := failover.NewController(database, engine, balanceManager, telemetryManager, cfg.PollInterval, logger)
	telemetryManager.SetFailoverEvidenceProcessor(failoverController)
	failoverController.Start(ctx)
	telemetryManager.Start(ctx)
	var agentSecretBox *balance.SecretBox
	if len(cfg.AgentCredentialKey) > 0 {
		agentSecretBox, err = balance.NewSecretBox(cfg.AgentCredentialKey)
		if err != nil {
			logger.Error("agent_credential_key_invalid", "error", err)
			os.Exit(1)
		}
	}
	if providerCount, countErr := database.CountConfiguredAgentProviders(context.Background()); countErr != nil {
		logger.Error("agent_provider_count_failed", "error", countErr)
		os.Exit(1)
	} else if providerCount > 0 && agentSecretBox == nil {
		logger.Error("agent_credential_key_required", "error", "AGENT_CREDENTIAL_KEY is required when model providers exist")
		os.Exit(1)
	}
	agentManager := agent.NewManager(database, engine, balanceManager, agentSecretBox, logger, telemetryManager)
	agentManager.Start(ctx)

	server := httpserver.New(cfg, database, engine, balanceManager, client, logger, agentManager)
	httpServer := &http.Server{Addr: cfg.ListenAddress, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		logger.Info("server_started", "listen", cfg.ListenAddress, "poll_interval", cfg.PollInterval.String(), "telemetry_poll_interval", cfg.TelemetryPollInterval.String(), "base_path", cfg.BasePath)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_failed", "error", err)
			cancel()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	logger.Info("server_stopped")
}
