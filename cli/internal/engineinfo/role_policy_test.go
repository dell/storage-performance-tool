package engineinfo

import "testing"

func TestParticipantRolePolicyNamesOrderingAndTopologyRules(t *testing.T) {
	tests := []struct {
		name string
		role ParticipantRole
		want participantRolePolicy
	}{
		{
			name: "entry",
			role: RoleEntry,
			want: participantRolePolicy{canonicalOrderRank: 0, countsAsEntry: true},
		},
		{
			name: "worker",
			role: RoleWorker,
			want: participantRolePolicy{canonicalOrderRank: 1},
		},
		{
			name: "standalone",
			role: RoleStandalone,
			want: participantRolePolicy{canonicalOrderRank: 0, mustBeOnlyParticipant: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := policyForParticipantRole(test.role)
			if !ok || got != test.want {
				t.Fatalf("policyForParticipantRole(%q) = %+v, %t; want %+v, true",
					test.role, got, ok, test.want)
			}
		})
	}

	if got, ok := policyForParticipantRole(ParticipantRole("container")); ok || got != (participantRolePolicy{}) {
		t.Fatalf("unsupported role policy = %+v, %t; want zero, false", got, ok)
	}
}
