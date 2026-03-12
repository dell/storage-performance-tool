package com.dell.spt.base.svc;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that every method reachable through an RMI-exported service interface is declared in an
 * interface that extends {@link Remote}.
 *
 * <p>Java RMI's {@code RemoteObjectInvocationHandler} checks {@code
 * Remote.class.isAssignableFrom(method.getDeclaringClass())} for every proxy invocation. If a
 * method is inherited from a non-{@code Remote} interface (e.g. {@code Closeable.close()}), the
 * call fails at runtime with {@code RemoteException: Method is not Remote}. This test catches such
 * gaps at build time.
 *
 * <p>Regression guard for the distributed-mode RMI bug fixed in v5.4.0 where {@code
 * AsyncRunnable} lost {@code extends Remote} and worker nodes could not execute step slices.
 */
class RmiRemoteMethodComplianceTest {

	/** All {@link Service} sub-interfaces that are exported as RMI remote objects. */
	private static final List<Class<? extends Service>> RMI_SERVICE_INTERFACES = List.of(LoadStepService.class, LoadStepManagerService.class, FileManagerService.class);

	@Test
	void allRmiServiceMethodsDeclaredInRemoteInterfaces() {
		var violations = new ArrayList<String>();

		for (var serviceIface : RMI_SERVICE_INTERFACES) {
			for (Method method : serviceIface.getMethods()) {
				Class<?> declaringClass = method.getDeclaringClass();
				if (declaringClass == Object.class) {
					continue; // Object methods are handled specially by the RMI invocation handler
				}
				if (!Remote.class.isAssignableFrom(declaringClass)) {
					violations.add(
									serviceIface.getSimpleName()
													+ "::"
													+ method.getName()
													+ "() — declared in "
													+ declaringClass.getName()
													+ " which does not extend Remote");
				}
			}
		}

		assertTrue(
						violations.isEmpty(),
						"Methods declared in non-Remote interfaces will fail at RMI invocation time "
										+ "with 'RemoteException: Method is not Remote'. "
										+ "Either the declaring interface must extend Remote, "
										+ "or the method must be redeclared in a Remote-extending interface.\n"
										+ "Violations:\n  "
										+ String.join("\n  ", violations));
	}
}
