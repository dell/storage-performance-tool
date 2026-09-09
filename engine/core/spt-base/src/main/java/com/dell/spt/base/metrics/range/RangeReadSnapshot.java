package com.dell.spt.base.metrics.range;

import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;

/** Additive range-only results. Live snapshots are not transactional or terminal reconciliation proof. */
public record RangeReadSnapshot(int schemaVersion,RangeReadPolicy policy,OperationLifecycleCounters logical,long attempted,long requestsSent,long successfulBytes,long localSelectionErrors,long httpFailures,long responseValidationFailures,long transportFailures,long httpAttemptFailures,long responseValidationAttemptFailures,long transportAttemptFailures,long failedReceivedBytes,long unresolvedReceivedBytes,boolean overflow){
/** Checked terminal-only identities: overflow can never qualify as exact accounting. */
public boolean reconciled(){if(schemaVersion!=1||overflow||!logical.reconciled()){return false;}if(attempted<0||requestsSent<0||successfulBytes<0||localSelectionErrors<0||httpFailures<0||responseValidationFailures<0||transportFailures<0||httpAttemptFailures<0||responseValidationAttemptFailures<0||transportAttemptFailures<0||failedReceivedBytes<0||unresolvedReceivedBytes<0){return false;}try{final long finalFailures=Math.addExact(Math.addExact(localSelectionErrors,httpFailures),Math.addExact(responseValidationFailures,transportFailures));final long terminal=Math.addExact(logical.accepted(),logical.failed());return finalFailures==logical.failed()&&successfulBytes==Math.multiplyExact(logical.accepted(),policy.length())&&logical.selected()==Math.addExact(terminal,Math.addExact(logical.unattempted(),logical.unresolved()))&&attempted==Math.addExact(terminal,logical.unresolved());}catch(ArithmeticException overflowed){return false;}}}
