package com.dell.spt.base.storage.driver;

/**
 * Marks a storage driver that does not run composite (multipart) operations: the CREATE and READ
 * operations an {@code item-data-ranges-threshold} above 0 makes for items larger than it. A load
 * step checks for this marker once, at start, and refuses such a configuration on a driver whose
 * class declares it: see {@link CompositeOperationSupport#requireSupport}.
 *
 * <p>The marker is opt-out and has no methods, so a driver built outside the engine keeps the
 * behaviour it had before, whatever methods it declares. Only the driver's own runtime class counts,
 * and only when that class names this marker in its own {@code implements} clause:
 *
 * <ul>
 *   <li>A subclass of a driver that declares it, built outside the engine, is not refused. A subclass
 *       that has no composite support either must declare this marker itself.
 *   <li>A driver that delegates to another driver (a wrapper or decorator) is not refused, even when
 *       the driver it wraps declares the marker, unless the wrapper's class declares it.
 * </ul>
 */
public interface CompositeOperationsUnsupported {}
