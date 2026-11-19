package com.dell.spt.base.storage.driver;

/**
 * Marks a storage driver that does not run composite (multipart) operations: the CREATE and READ
 * operations an {@code item-data-ranges-threshold} above 0 makes for items larger than it. A load
 * step checks for this marker once, at start, and refuses such a configuration on a driver that
 * implements it: see {@link CompositeOperationSupport#requireSupport}.
 *
 * <p>The marker is opt-out and has no methods, so a driver built outside the engine keeps the
 * behaviour it had before, whatever methods it declares. A driver that delegates to another driver
 * (a wrapper or decorator) is checked by its own class only: it still reports composite support
 * unless it implements this marker itself, even when the driver it wraps implements it.
 */
public interface CompositeOperationsUnsupported {}
