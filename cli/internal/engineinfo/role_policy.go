package engineinfo

// participantRolePolicy is the single interpretation of a planned topology
// role for validation, canonical ordering, and participant inclusion rules.
type participantRolePolicy struct {
	canonicalOrderRank    int
	countsAsEntry         bool
	mustBeOnlyParticipant bool
}

func policyForParticipantRole(role ParticipantRole) (participantRolePolicy, bool) {
	switch role {
	case RoleEntry:
		return participantRolePolicy{canonicalOrderRank: 0, countsAsEntry: true}, true
	case RoleWorker:
		return participantRolePolicy{canonicalOrderRank: 1}, true
	case RoleStandalone:
		return participantRolePolicy{canonicalOrderRank: 0, mustBeOnlyParticipant: true}, true
	default:
		return participantRolePolicy{}, false
	}
}
