package tui

import "testing"

func TestSelectHostIconByPhase(t *testing.T) {
	cases := []struct {
		name   string
		status NodeConnectionStatus
		icon   string
	}{
		{
			name:   "pending-is-disconnected",
			status: NodeConnectionStatus{Phase: NodePhasePending},
			icon:   "❌",
		},
		{
			name:   "contacted-uses-blue",
			status: NodeConnectionStatus{Phase: NodePhaseContacted},
			icon:   "🔵",
		},
		{
			name:   "container-starting-stays-blue",
			status: NodeConnectionStatus{Phase: NodePhaseContainerStarting, IsConnected: true},
			icon:   "🔵",
		},
		{
			name:   "api-ready-connected-green",
			status: NodeConnectionStatus{Phase: NodePhaseAPIReady, IsConnected: true},
			icon:   "✅",
		},
		{
			name:   "metrics-flowing-green",
			status: NodeConnectionStatus{Phase: NodePhaseMetricsFlowing, IsConnected: true},
			icon:   "✅",
		},
		{
			name:   "api-ready-disconnected-blue",
			status: NodeConnectionStatus{Phase: NodePhaseAPIReady, IsConnected: false},
			icon:   "🔵",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			icon := selectHostIcon(tc.status)
			if icon != tc.icon {
				t.Fatalf("expected icon %s, got %s", tc.icon, icon)
			}
		})
	}
}
