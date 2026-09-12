/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

type workerContextManager struct {
	*RemoteDockerManager
	check func(context.Context)
}

func (m *workerContextManager) StartWorkerNodeContainerContext(ctx context.Context, image, ip string, port, count int, args []string) (string, error) {
	m.check(ctx)
	return m.RemoteDockerManager.StartWorkerNodeContainerContext(ctx, image, ip, port, count, args)
}

func TestStartWorkerNodePhaseContexts(t *testing.T) {
	for _, mode := range []string{"success", "caller deadline", "cancel detection", "cancel startup", "cancel readiness", "detection failure", "empty address"} {
		t.Run(mode, func(t *testing.T) {
			mgr, _, info := newTestRemoteManager(t)
			caller, cancel := context.WithCancel(context.Background())
			if mode == "caller deadline" {
				cancel()
				caller, cancel = context.WithTimeout(context.Background(), time.Hour)
			}
			defer cancel()
			var detection context.Context
			var detectionDeadline time.Time
			started, probed := false, false
			detectionErr := errors.New("detection failed")
			o := &MultiHostOrchestrator{image: constants.DefaultSptImage}
			o.detectAdvIP = func(ctx context.Context, _ *hostparse.HostInfo) (string, error) {
				detection = ctx
				var ok bool
				detectionDeadline, ok = ctx.Deadline()
				if !ok || time.Until(detectionDeadline) > constants.AdvertisedIPDetectionTimeout {
					t.Fatal("IP detection must have its bounded deadline")
				}
				if mode == "cancel detection" {
					cancel()
					return "", ctx.Err()
				}
				if mode == "detection failure" {
					return "", detectionErr
				}
				if mode == "empty address" {
					return " ", nil
				}
				return "192.0.2.10", nil
			}
			wrapped := &workerContextManager{RemoteDockerManager: mgr, check: func(ctx context.Context) {
				started = true
				if ctx != caller {
					t.Error("container startup did not receive the original caller context")
				}
				if detection.Err() != context.Canceled {
					t.Error("detection context was not released before startup")
				}
				if mode == "cancel startup" {
					cancel()
				}
			}}
			mgr.proberRun = func(ctx context.Context, _ string, _ time.Duration) error {
				probed = true
				deadline, ok := ctx.Deadline()
				if !ok || !deadline.After(detectionDeadline) || time.Until(deadline) > constants.APIReadinessTimeout {
					t.Error("readiness did not receive its independent phase deadline")
				}
				if mode == "cancel readiness" {
					cancel()
				}
				return ctx.Err()
			}
			host := &HostConnection{Info: info, DockerManager: wrapped}
			err := o.startWorkerNode(caller, host, nil)
			switch mode {
			case "cancel detection", "cancel startup", "cancel readiness":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want caller cancellation", err)
				}
			case "detection failure":
				if !errors.Is(err, detectionErr) {
					t.Fatalf("error = %v, want detection failure", err)
				}
			case "empty address":
				if err == nil {
					t.Fatal("expected empty address failure")
				}
			default:
				if err != nil || !started || !probed || host.Status != HostStatusRunning {
					t.Fatalf("worker did not become ready: started=%v probed=%v err=%v", started, probed, err)
				}
			}
			if (mode == "cancel detection" || mode == "detection failure" || mode == "empty address") && started {
				t.Fatal("container started after failed detection")
			}
		})
	}
}
