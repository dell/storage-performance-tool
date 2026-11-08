import axios from "axios";
import type { AxiosRequestConfig, AxiosResponse } from "axios";

export const apiClient = axios.create({
  headers: { Accept: "application/json" },
});

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status?: number,
    cause?: unknown,
  ) {
    super(message, { cause });
    this.name = "ApiError";
  }
}

function isJSON(response: AxiosResponse<unknown>): boolean {
  return String(response.headers["content-type"] ?? "")
    .toLowerCase()
    .includes("application/json");
}

function responseError(
  response: AxiosResponse<unknown>,
  fallback: string,
): ApiError {
  if (!isJSON(response))
    return new ApiError(
      `${fallback} The dashboard server did not answer (HTTP ${response.status}).`,
      response.status,
    );
  const body = response.data;
  const message =
    typeof body === "object" &&
    body !== null &&
    "error" in body &&
    typeof body.error === "string"
      ? body.error
      : `${fallback} (HTTP ${response.status}).`;
  return new ApiError(message, response.status);
}

export async function request<T>(
  config: AxiosRequestConfig<unknown>,
  fallback: string,
): Promise<T> {
  try {
    const response = await apiClient.request<T>(config);
    if (!isJSON(response)) throw responseError(response, fallback);
    if (
      typeof response.data !== "object" ||
      response.data === null ||
      Array.isArray(response.data)
    )
      throw new ApiError(
        `${fallback} The server returned an unsupported dashboard response.`,
        response.status,
      );
    return response.data;
  } catch (error) {
    // Cancellation must be recognizable to callers and TanStack Query
    if (axios.isCancel(error)) throw error;
    if (axios.isAxiosError<unknown>(error)) {
      if (error.response) throw responseError(error.response, fallback);
      throw new ApiError(
        `${fallback} Could not reach the dashboard server.`,
        undefined,
        error,
      );
    }
    throw error;
  }
}
