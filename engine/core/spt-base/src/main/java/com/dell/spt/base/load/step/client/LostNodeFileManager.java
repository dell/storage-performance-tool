package com.dell.spt.base.load.step.client;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import java.io.EOFException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * A worker's file manager as the entry's file collectors see it: each call goes to the worker until
 * the entry loses that worker's step slice, and from then on none does. The entry then collects
 * none of that node's files: a read finds the end of the file at once, as a {@link NotCollected},
 * a size is 0, a delete does nothing, and any other call fails at once without reaching the worker.
 * The first such call of a node runs its {@code onFirstSkip}, once. The step fails for its lost
 * slice anyway, and a call to a frozen worker would wait for its connection's handshake.
 * The proxy is a {@link FileManagerService} when the file manager it wraps is one, so that a
 * collector still tells a worker's file manager from the entry's own one.
 */
final class LostNodeFileManager implements InvocationHandler {

	private final FileManager worker;
	private final BooleanSupplier lost;
	private final Runnable onFirstSkip;
	private final AtomicBoolean skipped = new AtomicBoolean();

	private LostNodeFileManager(final FileManager worker, final BooleanSupplier lost, final Runnable onFirstSkip) {
		this.worker = worker;
		this.lost = lost;
		this.onFirstSkip = onFirstSkip;
	}

	/**
	 * The end of a lost node's file, which the entry did not read: collectors that take an empty
	 * file for a failure can tell it from an empty or missing one.
	 */
	static final class NotCollected extends EOFException {
		private static final long serialVersionUID = 1L;

		NotCollected() {
			super("not collected: the entry lost this node's step slice");
		}
	}

	/**
	 * Guards a worker's file manager.
	 *
	 * @param worker a worker's file manager
	 * @param lost whether the entry has lost that worker's step slice
	 * @param onFirstSkip run once, at the first call that the guard keeps from the worker
	 * @return the file manager that stops calling the worker once its slice is lost
	 */
	static FileManager guard(final FileManager worker, final BooleanSupplier lost, final Runnable onFirstSkip) {
		final Class<?>[] interfaces = worker instanceof FileManagerService
						? new Class<?>[]{FileManagerService.class
						}
						: new Class<?>[]{FileManager.class
						};
		return (FileManager) Proxy.newProxyInstance(
						LostNodeFileManager.class.getClassLoader(), interfaces, new LostNodeFileManager(worker, lost, onFirstSkip));
	}

	@Override
	public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
		if (method.getDeclaringClass() == Object.class) {
			return switch (method.getName()) {
			case "equals" -> proxy == args[0];
			case "hashCode" -> System.identityHashCode(proxy);
			default -> worker.toString();
			};
		}
		if (lost.getAsBoolean()) {
			if (skipped.compareAndSet(false, true)) {
				onFirstSkip.run();
			}
			return switch (method.getName()) {
			case "readFromFile" -> throw new NotCollected();
			case "fileSize" -> 0L;
			case "deleteFile" -> null;
			default -> throw new RemoteException(
							"not called: the entry lost this node's step slice (" + method.getName() + ")");
			};
		}
		try {
			return method.invoke(worker, args);
		} catch (final InvocationTargetException e) {
			throw e.getCause();
		}
	}
}
