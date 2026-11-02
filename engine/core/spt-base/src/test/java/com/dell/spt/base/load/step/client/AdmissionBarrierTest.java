package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.load.step.PreparationStatus.State;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.service.LoadStepServiceImpl;
import com.dell.spt.base.svc.ServiceUtil;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class AdmissionBarrierTest {

	private final AtomicLong now = new AtomicLong(1_000);
	private final AdmissionBarrier barrier = new AdmissionBarrier(
					"read1", TimeUnit.MINUTES.toNanos(10), "10m", now::get, 1);

	private static PreparationStatus status(final State state, final String phase, final String message) {
		return new PreparationStatus(state, phase, message, TimeUnit.SECONDS.toNanos(9));
	}

	private LoadStep slice(final PreparationStatus status) throws RemoteException {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.preparationStatus(barrier.id())).thenReturn(status);
		when(slice.releasePreparedAdmission(barrier.id())).thenReturn(true);
		return slice;
	}

	@Test
	void releasesEverySliceOnlyWhenAllAreReady() throws Exception {
		final LoadStep entry = slice(status(State.READY, "ready", ""));
		final LoadStep worker = mock(LoadStep.class);
		when(worker.preparationStatus(barrier.id()))
						.thenReturn(status(State.RUNNING, "probing paths", ""))
						.thenReturn(status(State.READY, "ready", ""));
		when(worker.releasePreparedAdmission(barrier.id())).thenReturn(true);
		barrier.arm("entry", entry);
		barrier.arm("198.51.100.12:1099", worker);

		final var millis = barrier.awaitAndRelease();

		verify(entry).startPreparation(eq(barrier.id()), anyLong());
		verify(worker).startPreparation(eq(barrier.id()), anyLong());
		verify(entry).releasePreparedAdmission(barrier.id());
		verify(worker).releasePreparedAdmission(barrier.id());
		verify(entry, never()).fencePreparation(anyLong());
		assertEquals(9_000L, millis.get("entry"));
		assertEquals(9_000L, millis.get("198.51.100.12:1099"));
	}

	@Test
	void aFailedEngineFencesEverySliceAndNamesEachFailure() throws Exception {
		final LoadStep entry = slice(status(State.READY, "ready", ""));
		final LoadStep bad = slice(status(State.FAILED, "probing paths", "no route to 192.0.2.1"));
		barrier.arm("entry", entry);
		barrier.arm("198.51.100.12:1099", bad);

		final var failure = assertThrows(RunTerminalException.class, barrier::awaitAndRelease);

		assertEquals(
						"Step read1 did not start: 1 of 2 engines failed preparation — 198.51.100.12:1099: no route to 192.0.2.1",
						failure.getMessage());
		verify(entry).fencePreparation(barrier.id());
		verify(bad).fencePreparation(barrier.id());
		verify(entry, never()).releasePreparedAdmission(anyLong());
	}

	@Test
	void theDeadlineFencesEverySliceAndNamesWhatIsStillRunning() throws Exception {
		final LoadStep worker = slice(status(State.RUNNING, "pre-warming the pool", ""));
		barrier.arm("198.51.100.12:1099", worker);
		now.addAndGet(TimeUnit.MINUTES.toNanos(11));

		final var failure = assertThrows(RunTerminalException.class, barrier::awaitAndRelease);

		assertEquals(
						"Step read1 did not start: preparation did not finish within 10m (load.step.preparation.timeout) — "
										+ "198.51.100.12:1099: still pre-warming the pool",
						failure.getMessage());
		verify(worker).fencePreparation(barrier.id());
	}

	@Test
	void everyEngineBecomingReadyOnlyAfterTheDeadlineStillFails() throws Exception {
		final LoadStep worker = slice(status(State.READY, "ready", ""));
		barrier.arm("198.51.100.12:1099", worker);
		now.addAndGet(TimeUnit.MINUTES.toNanos(11));

		final var failure = assertThrows(RunTerminalException.class, barrier::awaitAndRelease);

		assertTrue(failure.getMessage().endsWith("every engine became ready only after it"), failure.getMessage());
		verify(worker, never()).releasePreparedAdmission(anyLong());
		verify(worker).fencePreparation(barrier.id());
	}

	@Test
	void aWorkerWithoutTheMethodsIsNamed() throws Exception {
		final String want = "Step read1 did not start: the engine on 198.51.100.12:1099 does not support the admission "
						+ "barrier, which a step with a NIC set needs; run the same SPT image on every host";
		final LoadStep oldBuild = mock(LoadStep.class);
		doThrow(new UnmarshalException("unrecognized method hash")).when(oldBuild).startPreparation(anyLong(), anyLong());
		assertEquals(want, assertThrows(RunTerminalException.class, () -> barrier.arm("198.51.100.12:1099", oldBuild)).getMessage());

		final LoadStep defaultMethod = mock(LoadStep.class);
		doThrow(new RemoteException(LoadStep.NO_ADMISSION_BARRIER)).when(defaultMethod).startPreparation(anyLong(), anyLong());
		assertEquals(want, assertThrows(RunTerminalException.class, () -> barrier.arm("198.51.100.12:1099", defaultMethod)).getMessage());
	}

	@Test
	void aSliceThatCannotBeReleasedStopsTheStep() throws Exception {
		final LoadStep good = slice(status(State.READY, "ready", ""));
		final LoadStep refuses = slice(status(State.READY, "ready", ""));
		when(refuses.releasePreparedAdmission(barrier.id())).thenReturn(false);
		final LoadStep unreachable = slice(status(State.READY, "ready", ""));
		when(unreachable.releasePreparedAdmission(barrier.id())).thenThrow(new RemoteException("connection refused"));
		barrier.arm("entry", good);
		barrier.arm("198.51.100.12:1099", refuses);

		final var failure = assertThrows(RunTerminalException.class, barrier::awaitAndRelease);
		assertEquals(
						"Step read1 stopped: admission could not be released on 198.51.100.12:1099: its preparation was fenced",
						failure.getMessage());
		verify(good).fencePreparation(barrier.id());

		final AdmissionBarrier second = new AdmissionBarrier("read1", TimeUnit.MINUTES.toNanos(10), "10m", now::get, 1);
		final LoadStep other = mock(LoadStep.class);
		when(other.preparationStatus(second.id())).thenReturn(status(State.READY, "ready", ""));
		when(other.releasePreparedAdmission(second.id())).thenThrow(new RemoteException("connection refused"));
		second.arm("198.51.100.13:1099", other);
		final var thrown = assertThrows(RunTerminalException.class, second::awaitAndRelease);
		assertTrue(thrown.getMessage().startsWith("Step read1 stopped: admission could not be released on 198.51.100.13:1099"),
						thrown.getMessage());
	}

	@Test
	void anInterruptFencesEverySlice() throws Exception {
		final LoadStep worker = slice(status(State.RUNNING, "probing paths", ""));
		barrier.arm("198.51.100.12:1099", worker);
		Thread.currentThread().interrupt();
		assertThrows(InterruptedException.class, barrier::awaitAndRelease);
		verify(worker).fencePreparation(barrier.id());
	}

	@Test
	void aSliceThatCannotBeFencedDoesNotStopTheOthersBeingFenced() throws Exception {
		final LoadStep unreachable = slice(status(State.FAILED, "starting", "gone"));
		doThrow(new RemoteException("connection refused")).when(unreachable).fencePreparation(anyLong());
		final LoadStep other = slice(status(State.READY, "ready", ""));
		barrier.arm("198.51.100.12:1099", unreachable);
		barrier.arm("198.51.100.13:1099", other);
		assertThrows(RunTerminalException.class, barrier::awaitAndRelease);
		verify(other).fencePreparation(barrier.id());
	}

	@Test
	void itemDistributionRunsWithinTheDeadlineAndIsFencedAtIt() throws Exception {
		final var quick = new AdmissionBarrier("read1", TimeUnit.MINUTES.toNanos(1), "1m");
		final var ran = new AtomicBoolean();
		quick.distribute(() -> ran.set(true));
		assertTrue(ran.get());

		final var slow = new AdmissionBarrier("read1", TimeUnit.MILLISECONDS.toNanos(50), "50ms");
		final var interrupted = new CountDownLatch(1);
		final var failure = assertThrows(RunTerminalException.class, () -> slow.distribute(() -> {
			try {
				TimeUnit.MINUTES.sleep(1);
			} catch (final InterruptedException e) {
				interrupted.countDown();
			}
		}));
		assertEquals(
						"Step read1 did not start: preparation did not finish within 50ms (load.step.preparation.timeout) — "
										+ "entry: still distributing items",
						failure.getMessage());
		assertTrue(interrupted.await(10, TimeUnit.SECONDS), "a late distribution is interrupted, so it writes no more");
	}

	@Test
	void aFailingDistributionFailsWithItsOwnException() {
		final var boom = new IllegalArgumentException("bad list");
		assertEquals(boom, assertThrows(IllegalArgumentException.class, () -> barrier.distribute(() -> {
			throw boom;
		})));
		assertFalse(Thread.currentThread().isInterrupted());
	}

	/** The barrier's four calls and their record cross real RMI, as they do to a worker. */
	@Test
	void theBarrierReachesAWorkerThroughRealRmi() throws Exception {
		final LoadStep worker = mock(LoadStep.class);
		when(worker.loadStepId()).thenReturn("real-rmi-barrier");
		final var barrierOverRmi = new AdmissionBarrier("read1", TimeUnit.MINUTES.toNanos(10), "10m", System::nanoTime, 1);
		when(worker.preparationStatus(barrierOverRmi.id()))
						.thenReturn(new PreparationStatus(State.RUNNING, "probing paths", "", 5))
						.thenReturn(new PreparationStatus(State.READY, "ready", "", TimeUnit.SECONDS.toNanos(2)));
		when(worker.releasePreparedAdmission(barrierOverRmi.id())).thenReturn(true);
		withRealService(worker, remote -> {
			barrierOverRmi.arm("127.0.0.1:1099", remote);
			final var millis = barrierOverRmi.awaitAndRelease();
			assertEquals(2_000L, millis.get("127.0.0.1:1099"));
			verify(worker).startPreparation(eq(barrierOverRmi.id()), anyLong());
			verify(worker).releasePreparedAdmission(barrierOverRmi.id());
		});
	}

	@Test
	void aFailureAndItsFenceCrossRealRmi() throws Exception {
		final LoadStep worker = mock(LoadStep.class);
		when(worker.loadStepId()).thenReturn("real-rmi-barrier-failure");
		final var barrierOverRmi = new AdmissionBarrier("read1", TimeUnit.MINUTES.toNanos(10), "10m", System::nanoTime, 1);
		when(worker.preparationStatus(barrierOverRmi.id()))
						.thenReturn(new PreparationStatus(State.FAILED, "probing paths", "no route", 5));
		withRealService(worker, remote -> {
			barrierOverRmi.arm("127.0.0.1:1099", remote);
			final var failure = assertThrows(RunTerminalException.class, barrierOverRmi::awaitAndRelease);
			assertEquals(
							"Step read1 did not start: 1 of 1 engines failed preparation — 127.0.0.1:1099: no route",
							failure.getMessage());
			verify(worker).fencePreparation(barrierOverRmi.id());
		});
	}

	@Test
	void aWorkerStepWithoutTheMethodsIsNamedThroughRealRmi() throws Exception {
		final LoadStep worker = mock(LoadStep.class);
		when(worker.loadStepId()).thenReturn("real-rmi-barrier-old");
		doThrow(new RemoteException(LoadStep.NO_ADMISSION_BARRIER)).when(worker).startPreparation(anyLong(), anyLong());
		withRealService(worker, remote -> {
			final var failure = assertThrows(RunTerminalException.class, () -> barrier.arm("127.0.0.1:1099", remote));
			assertTrue(failure.getMessage().contains("does not support the admission barrier"), failure.getMessage());
		});
	}

	@FunctionalInterface
	private interface RemoteAction {
		void run(LoadStepService remote) throws Exception;
	}

	private static void withRealService(final LoadStep local, final RemoteAction action) throws Exception {
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		LoadStepServiceImpl service = null;
		try {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			service = LoadStepClientBaseTest.newRealDurationService(local, port);
			action.run(ServiceUtil.resolve("127.0.0.1", port, service.name(), LoadStepService.class));
		} finally {
			if (service != null && !service.isClosed()) {
				service.close();
			}
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}
}
