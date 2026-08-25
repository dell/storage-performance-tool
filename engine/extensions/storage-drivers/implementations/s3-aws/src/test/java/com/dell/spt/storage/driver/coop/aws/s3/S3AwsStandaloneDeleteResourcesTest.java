package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

final class S3AwsStandaloneDeleteResourcesTest {

	@Test
	void concurrentFirstUseCreatesOneSafelyPublishedResourceSet() throws Exception {
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
		final var creations = new AtomicInteger();
		final var lazy = new S3AwsStandaloneDeleteResources.Lazy(() -> {
			creations.incrementAndGet();
			return new S3AwsStandaloneDeleteResources(client, httpClient);
		});
		final int callerCount = 32;
		final var start = new CountDownLatch(1);
		final List<S3AsyncClient> observed = new ArrayList<>();
		try (final var callers = Executors.newVirtualThreadPerTaskExecutor()) {
			final var futures = new ArrayList<java.util.concurrent.Future<S3AsyncClient>>();
			for (int i = 0; i < callerCount; i++) {
				futures.add(callers.submit(() -> {
					assertTrue(start.await(3, TimeUnit.SECONDS));
					return lazy.client();
				}));
			}
			start.countDown();
			for (final var future : futures) {
				observed.add(future.get(3, TimeUnit.SECONDS));
			}
		}

		assertEquals(1, creations.get());
		observed.forEach(actual -> assertSame(client, actual));
		lazy.close();
		verify(client).close();
		verify(httpClient).close();
	}

	@Test
	void closeBeforeFirstUsePreventsLateCreationAndDuplicateCloseIsHarmless() {
		final var creations = new AtomicInteger();
		final var lazy = new S3AwsStandaloneDeleteResources.Lazy(() -> {
			creations.incrementAndGet();
			return new S3AwsStandaloneDeleteResources(
							mock(S3AsyncClient.class), mock(SdkAsyncHttpClient.class));
		});

		lazy.close();
		lazy.close();

		assertThrows(IllegalStateException.class, lazy::client);
		assertEquals(0, creations.get());
	}

	@Test
	void closeRacingFirstUseCannotLeakOrCreateAnotherResourceSet() throws Exception {
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
		final var creations = new AtomicInteger();
		final var supplierStarted = new CountDownLatch(1);
		final var releaseSupplier = new CountDownLatch(1);
		final var observedClient = new AtomicReference<S3AsyncClient>();
		final var lazy = new S3AwsStandaloneDeleteResources.Lazy(() -> {
			creations.incrementAndGet();
			supplierStarted.countDown();
			try {
				assertTrue(releaseSupplier.await(3, TimeUnit.SECONDS));
			} catch (final InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw new AssertionError(interrupted);
			}
			return new S3AwsStandaloneDeleteResources(client, httpClient);
		});
		try (final var callers = Executors.newVirtualThreadPerTaskExecutor()) {
			final var firstUse = callers.submit(() -> observedClient.set(lazy.client()));
			assertTrue(supplierStarted.await(3, TimeUnit.SECONDS));
			final var close = callers.submit(lazy::close);
			releaseSupplier.countDown();
			firstUse.get(3, TimeUnit.SECONDS);
			close.get(3, TimeUnit.SECONDS);
		}

		assertSame(client, observedClient.get());
		assertEquals(1, creations.get());
		assertThrows(IllegalStateException.class, lazy::client);
		verify(client).close();
		verify(httpClient).close();
	}

	@Test
	void closeReleasesInstantiatedResourcesOnceInReverseOwnershipOrder() {
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
		final var lazy = new S3AwsStandaloneDeleteResources.Lazy(
						() -> new S3AwsStandaloneDeleteResources(client, httpClient));
		assertSame(client, lazy.client());

		lazy.close();
		lazy.close();

		final var order = inOrder(client, httpClient);
		order.verify(client).close();
		order.verify(httpClient).close();
		verify(client, times(1)).close();
		verify(httpClient, times(1)).close();
	}

	@Test
	void closeAggregatesFailuresWithoutSkippingLaterResources() {
		final S3AsyncClient client = mock(S3AsyncClient.class);
		final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
		final var clientFailure = new IllegalStateException("client close failed");
		final var httpFailure = new IllegalArgumentException("HTTP close failed");
		org.mockito.Mockito.doThrow(clientFailure).when(client).close();
		org.mockito.Mockito.doThrow(httpFailure).when(httpClient).close();
		final var resources = new S3AwsStandaloneDeleteResources(client, httpClient);

		final RuntimeException thrown = assertThrows(RuntimeException.class, resources::close);

		assertSame(clientFailure, thrown);
		assertEquals(List.of(httpFailure), List.of(thrown.getSuppressed()));
		verify(client).close();
		verify(httpClient).close();
	}

	@Test
	void constructionFailureIsStickyAcrossConcurrentCallers() {
		final var attempts = new AtomicInteger();
		final var constructionFailure = new IllegalStateException("injected construction failure");
		final var lazy = new S3AwsStandaloneDeleteResources.Lazy(() -> {
			attempts.incrementAndGet();
			throw constructionFailure;
		});

		assertSame(constructionFailure, assertThrows(RuntimeException.class, lazy::client));
		assertSame(constructionFailure, assertThrows(RuntimeException.class, lazy::client));
		assertEquals(1, attempts.get());
		lazy.close();
	}

	@Test
	void partialConstructionFailureClosesHttpClientAndPreservesCloseFailure() {
		final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
		final var constructionFailure = new IllegalStateException("S3 client build failed");
		final var closeFailure = new IllegalArgumentException("HTTP close failed");
		org.mockito.Mockito.doThrow(closeFailure).when(httpClient).close();

		final RuntimeException thrown = assertThrows(
						RuntimeException.class,
						() -> S3AwsStandaloneDeleteResources.create(
										() -> httpClient,
										ignored -> {
											throw constructionFailure;
										}));

		assertSame(constructionFailure, thrown);
		assertEquals(List.of(closeFailure), List.of(thrown.getSuppressed()));
		verify(httpClient).close();
	}
}
