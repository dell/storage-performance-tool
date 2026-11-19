package com.dell.spt.base.load.step.client;

import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.confuse.Config;
import java.util.Set;

/**
 * Implemented by a load-step client whose operations are not of its {@code load-op-type}: the op
 * types a configuration makes, for the composite-support check the entry runs before it reads the
 * items. Without it, the check takes a DATA configuration's {@code load-op-type}, and nothing else.
 *
 * <p>It is an interface, not a method of {@link LoadStepClientBase}, so a client built outside the
 * engine, which may already declare a method of this name, compiles and runs as before.
 */
public interface CompositeCheckedOperations {

	/** The op types that {@code config} makes. */
	Set<OpType> compositeCheckedOpTypes(Config config);
}
