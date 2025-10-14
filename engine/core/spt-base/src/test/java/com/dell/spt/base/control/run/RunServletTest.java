package com.dell.spt.base.control.run;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.concurrent.SingleTaskExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Test for RunServlet focusing on:
 * 1. DELETE endpoint returning 200 OK instead of 500 (Phase 2)
 * 2. Proper error handling in stop operations
 * 3. If-Match header handling
 */
class RunServletTest {

	@Mock
	private Run mockRun;
	@Mock
	private SingleTaskExecutor mockScenarioExecutor;
	@Mock
	private HttpServletRequest mockRequest;
	@Mock
	private HttpServletResponse mockResponse;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
	}

	@Test
	void testStopRunIfMatches_RunIdMatches_ReturnsOk() {
		// Phase 2 fix: DELETE should return 200 OK when run ID matches
		long runId = 12345L;
		when(mockRun.runId()).thenReturn(runId);
		when(mockScenarioExecutor.task()).thenReturn(null); // Task cleared after stop

		RunServlet.stopRunIfMatchesAndSetResponse(mockRun, mockResponse, runId, mockScenarioExecutor);

		verify(mockScenarioExecutor, times(1)).stop(mockRun);
		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_OK);
		verify(mockResponse, never()).setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Test
	void testStopRunIfMatches_RunIdMatches_TaskStillActive_ReturnsOkWithWarning() {
		// Phase 2 fix: Even if task is still active after stop, should return OK with warning log
		long runId = 12345L;
		when(mockRun.runId()).thenReturn(runId);
		when(mockScenarioExecutor.task()).thenReturn(mockRun); // Task still active

		RunServlet.stopRunIfMatchesAndSetResponse(mockRun, mockResponse, runId, mockScenarioExecutor);

		verify(mockScenarioExecutor, times(1)).stop(mockRun);
		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_OK);
		verify(mockResponse, never()).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}

	@Test
	void testStopRunIfMatches_RunIdDoesNotMatch_ReturnsNotFound() {
		// Should return 404 when run ID doesn't match
		long runId = 12345L;
		long differentRunId = 67890L;
		when(mockRun.runId()).thenReturn(runId);

		RunServlet.stopRunIfMatchesAndSetResponse(mockRun, mockResponse, differentRunId, mockScenarioExecutor);

		verify(mockScenarioExecutor, never()).stop(any());
		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_NOT_FOUND);
		verify(mockResponse, never()).setStatus(HttpServletResponse.SC_OK);
	}

	@Test
	void testStopRunIfMatches_MultipleStopCallsWithSameId() {
		// Verify multiple stop calls with same ID are handled gracefully
		long runId = 12345L;
		when(mockRun.runId()).thenReturn(runId);
		when(mockScenarioExecutor.task()).thenReturn(null);

		// Call stop twice with same run ID
		RunServlet.stopRunIfMatchesAndSetResponse(mockRun, mockResponse, runId, mockScenarioExecutor);
		RunServlet.stopRunIfMatchesAndSetResponse(mockRun, mockResponse, runId, mockScenarioExecutor);

		// Both calls should succeed (return OK)
		verify(mockScenarioExecutor, times(2)).stop(mockRun);
		verify(mockResponse, times(2)).setStatus(HttpServletResponse.SC_OK);
	}

	@Test
	void testSetRunMatchesResponse_IdMatches() {
		// Verify GET operation with matching run ID
		long runId = 12345L;
		when(mockRun.runId()).thenReturn(runId);

		RunServlet.setRunMatchesResponse(mockRun, mockResponse, runId);

		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_OK);
	}

	@Test
	void testSetRunMatchesResponse_IdDoesNotMatch() {
		// Verify GET operation with non-matching run ID
		long runId = 12345L;
		long differentRunId = 67890L;
		when(mockRun.runId()).thenReturn(runId);

		RunServlet.setRunMatchesResponse(mockRun, mockResponse, differentRunId);

		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_NO_CONTENT);
	}

	@Test
	void testSetRunExistsResponse() {
		// Verify HEAD operation sets correct status and headers
		long runId = 12345L;
		when(mockRun.runId()).thenReturn(runId);

		RunServlet.setRunExistsResponse(mockRun, mockResponse);

		verify(mockResponse, times(1)).setStatus(HttpServletResponse.SC_OK);
		verify(mockResponse, times(1)).setHeader("ETAG", String.valueOf(runId));
	}

	// NEW TESTS FOR GET ENDPOINT BUG FIXES

	@Test
	void testExtractRequestTimestampAndApply_MissingIfMatchHeader_Returns400() throws IOException {
		// Test GET /run without If-Match header should return 400 Bad Request
		RunServlet servlet = createMockServlet();
		Enumeration<String> emptyHeaders = Collections.emptyEnumeration();
		when(mockRequest.getHeaders("If-Match")).thenReturn(emptyHeaders);

		servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {});

		verify(mockResponse, times(1)).sendError(
						HttpServletResponse.SC_BAD_REQUEST, "Missing header: If-Match");
	}

	@Test
	void testExtractRequestTimestampAndApply_InvalidRunIdFormat_Returns400() throws IOException {
		// Test GET /run with invalid (non-numeric) If-Match header should return 400
		RunServlet servlet = createMockServlet();
		Enumeration<String> invalidHeaders = Collections.enumeration(Collections.singletonList("invalid-run-id"));
		when(mockRequest.getHeaders("If-Match")).thenReturn(invalidHeaders);

		servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {});

		verify(mockResponse, times(1)).sendError(
						HttpServletResponse.SC_BAD_REQUEST, "Invalid run ID format: invalid-run-id");
	}

	@Test
	void testExtractRequestTimestampAndApply_ValidRunId_CallsConsumer() throws IOException {
		// Test GET /run with valid If-Match header processes correctly
		RunServlet servlet = createMockServlet();
		String validRunId = "12345";
		Enumeration<String> validHeaders = Collections.enumeration(Collections.singletonList(validRunId));
		when(mockRequest.getHeaders("If-Match")).thenReturn(validHeaders);
		when(mockRun.runId()).thenReturn(12345L);

		// Mock the applyForActiveRunIfAny behavior - simulate active run
		doAnswer(invocation -> {
			@SuppressWarnings("unchecked")
			java.util.function.BiConsumer<Run, HttpServletResponse> action = invocation.getArgument(1);
			action.accept(mockRun, mockResponse);
			return null;
		}).when(servlet).applyForActiveRunIfAny(eq(mockResponse), any());

		boolean[] consumerCalled = {false
		};
		servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {
			assertEquals(mockRun, run);
			assertEquals(12345L, runId.longValue());
			consumerCalled[0] = true;
		});

		assertTrue(consumerCalled[0], "Consumer should have been called with valid run ID");
		verify(mockResponse, never()).sendError(anyInt(), anyString());
	}

	@Test
	void testDoGet_MissingIfMatchHeader_Returns400() throws IOException {
		// Integration test: Verify doGet method properly handles missing If-Match header
		RunServlet servlet = createMockServlet();
		Enumeration<String> emptyHeaders = Collections.emptyEnumeration();
		when(mockRequest.getHeaders("If-Match")).thenReturn(emptyHeaders);

		servlet.doGet(mockRequest, mockResponse);

		verify(mockResponse, times(1)).sendError(
						HttpServletResponse.SC_BAD_REQUEST, "Missing header: If-Match");
	}

	@Test
	void testDoGet_InvalidRunIdFormat_Returns400() throws IOException {
		// Integration test: Verify doGet method properly handles invalid run ID format
		RunServlet servlet = createMockServlet();
		Enumeration<String> invalidHeaders = Collections.enumeration(Collections.singletonList("not-a-number"));
		when(mockRequest.getHeaders("If-Match")).thenReturn(invalidHeaders);

		servlet.doGet(mockRequest, mockResponse);

		verify(mockResponse, times(1)).sendError(
						HttpServletResponse.SC_BAD_REQUEST, "Invalid run ID format: not-a-number");
	}

	/**
	 * Helper method to create a RunServlet instance for testing.
	 * Uses minimal mocking to focus on the method under test.
	 */
	private RunServlet createMockServlet() {
		return spy(new RunServlet(
						null, // clsLoader - not needed for these tests
						Collections.emptyList(), // extensions - not needed
						null, // metricsMgr - not needed  
						null, // aggregatedConfigWithArgs - not needed
						null, // appHomePath - not needed
						null,  // scenarioStepSvc - not needed
						new com.dell.spt.base.control.ApiStatus()));
	}

	// ================================================================================
	// CRITICAL REGRESSION PREVENTION TESTS
	// These tests prevent the exact bug we just fixed from happening again
	// ================================================================================

	@Nested
	@DisplayName("Critical Regression Prevention Tests")
	class CriticalRegressionPrevention {

		@ParameterizedTest
		@ValueSource(strings = {"", "invalid-run-id", "not-a-number", "abc123", "12.34", "null", "undefined"
		})
		@DisplayName("CRITICAL: Client errors must NEVER return 500 (should be 400)")
		void testClientErrorsNeverReturn500(String ifMatchValue) throws IOException {
			// This test would have caught the original bug!
			// The bug: Long.parseLong(null) threw NumberFormatException -> 500 error
			// Expected: Should return 400 Bad Request for client mistakes

			RunServlet servlet = createMockServlet();

			if (ifMatchValue.isEmpty()) {
				// Test missing If-Match header
				when(mockRequest.getHeaders("If-Match")).thenReturn(Collections.emptyEnumeration());
			} else {
				// Test invalid If-Match header values
				when(mockRequest.getHeaders("If-Match"))
								.thenReturn(Collections.enumeration(List.of(ifMatchValue)));
			}

			// CRITICAL: This should NEVER throw an uncaught exception (which would be 500)
			assertDoesNotThrow(() -> {
				servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {});
			}, "Client error should not throw exception - should handle gracefully with 400 status");

			// Should always call sendError with 4xx status, never let exceptions escape
			verify(mockResponse, times(1)).sendError(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());
			verify(mockResponse, never()).sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), anyString());
		}

		@ParameterizedTest
		@CsvSource({
				"doGet, GET /run",
				"doDelete, DELETE /run"
		})
		@DisplayName("CRITICAL: All HTTP methods handle invalid headers gracefully")
		void testAllHttpMethodsHandleInvalidHeaders(String methodName, String description) throws Exception {
			// Test that both GET and DELETE handle missing If-Match header properly
			// This ensures consistent error handling across all endpoints

			RunServlet servlet = createMockServlet();
			when(mockRequest.getHeaders("If-Match")).thenReturn(Collections.emptyEnumeration());

			Method method = RunServlet.class.getDeclaredMethod(methodName,
							HttpServletRequest.class, HttpServletResponse.class);

			// Should not throw - should handle gracefully with 400
			assertDoesNotThrow(() -> {
				method.invoke(servlet, mockRequest, mockResponse);
			}, description + " should handle missing If-Match header gracefully");

			verify(mockResponse, times(1)).sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing header: If-Match");
		}

		@Test
		@DisplayName("CRITICAL: Realistic HTTP request parsing edge cases")
		void testRealisticHttpRequestParsingEdgeCases() throws IOException {
			// Test edge cases that occur in real HTTP processing
			RunServlet servlet = createMockServlet();

			// Edge Case 1: Headers collection that returns null when empty
			when(mockRequest.getHeaders("If-Match")).thenReturn(Collections.emptyEnumeration());

			assertDoesNotThrow(() -> {
				servlet.doGet(mockRequest, mockResponse);
			}, "Empty headers enumeration should be handled gracefully");

			verify(mockResponse, times(1)).sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing header: If-Match");

			// Reset for next test
			reset(mockResponse);

			// Edge Case 2: Headers with whitespace or special characters  
			when(mockRequest.getHeaders("If-Match"))
							.thenReturn(Collections.enumeration(Arrays.asList("  ", "\t", "\n")));

			assertDoesNotThrow(() -> {
				servlet.doGet(mockRequest, mockResponse);
			}, "Whitespace-only header values should be handled gracefully");

			// Should attempt to parse and fail gracefully
			verify(mockResponse, times(1)).sendError(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());
		}

		@ParameterizedTest
		@ValueSource(strings = {
				"9223372036854775808", // Long.MAX_VALUE + 1 (overflow)
				"-9223372036854775809", // Long.MIN_VALUE - 1 (underflow)  
				"1.0", // Float format
				"1e10", // Scientific notation
				"0x1234", // Hexadecimal
				"", // Empty string
				" ", // Space
				"null", // String "null"
				"undefined" // String "undefined"
		})
		@DisplayName("CRITICAL: Number parsing edge cases handled gracefully")
		void testNumberParsingEdgeCases(String invalidRunId) throws IOException {
			// Test various inputs that could cause Long.parseLong() to fail
			// These should all return 400 Bad Request, not 500 Internal Server Error

			RunServlet servlet = createMockServlet();
			when(mockRequest.getHeaders("If-Match"))
							.thenReturn(Collections.enumeration(List.of(invalidRunId)));

			assertDoesNotThrow(() -> {
				servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {});
			}, "Invalid run ID '" + invalidRunId + "' should not cause server error");

			verify(mockResponse, times(1)).sendError(eq(HttpServletResponse.SC_BAD_REQUEST),
							contains("Invalid run ID format: " + invalidRunId));
		}

		@Test
		@DisplayName("CRITICAL: Null header processing matches real HTTP behavior")
		void testNullHeaderProcessingRealism() throws IOException {
			// Simulate the exact conditions that caused the original bug
			RunServlet servlet = createMockServlet();

			// This simulates what happens when getHeaders() returns an enumeration
			// that produces null when findAny().orElse(null) is called
			when(mockRequest.getHeaders("If-Match")).thenReturn(new Enumeration<String>() {
				@Override
				public boolean hasMoreElements() {
					return false; // Empty enumeration
				}

				@Override
				public String nextElement() {
					return null; // Should never be called, but return null if it is
				}
			});

			// The original bug: this would try to parse null and throw NumberFormatException  
			assertDoesNotThrow(() -> {
				servlet.extractRequestTimestampAndApply(mockRequest, mockResponse, (run, runId) -> {});
			}, "Null header value should be detected before parsing");

			// Should detect null and return proper client error
			verify(mockResponse, times(1)).sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing header: If-Match");
			verify(mockResponse, never()).sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), anyString());
		}
	}
}
