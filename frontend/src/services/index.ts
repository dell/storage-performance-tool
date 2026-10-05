import { createMockSource } from "../data/mock";
import { createHttpSource } from "./http";
// Vite development defaults to local fixtures; production defaults to same-origin API.
export const isMock = import.meta.env.VITE_DATA_SOURCE
  ? import.meta.env.VITE_DATA_SOURCE === "mock"
  : !import.meta.env.PROD;
export const dataSource = isMock
  ? createMockSource()
  : createHttpSource(import.meta.env.VITE_API_BASE_URL || "/api");
