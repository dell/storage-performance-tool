package com.dell.spt.base.load.lifecycle;

/** Immutable operation counters, independent of DELETE object identities and recovery lists. */
public record OperationLifecycleCounters(
        boolean supported, long selected, long accepted, long failed, long terminalResults,
        long unattempted, long unresolved, long generatorBuffered, long driverQueued, long inFlight) {

    /** Terminal-only conservation, never a claim that a live snapshot is complete. */
    public boolean reconciled() {
        return supported && selected >= 0 && accepted >= 0 && failed >= 0
                && terminalResults >= 0 && unattempted >= 0 && unresolved >= 0
                && generatorBuffered == 0 && driverQueued == 0 && inFlight == 0
                && selected == accepted + failed + unattempted + unresolved
                && terminalResults == accepted + failed;
    }
}
