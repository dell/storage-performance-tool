import { useCallback, useEffect, useRef, useState } from "react";
import type { RunConfig, Snapshot } from "../types";
import { dataSource } from "../services";
export function useRun() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null),
    [error, setError] = useState<string | null>(null),
    [starting, setStarting] = useState(false);
  const generation = useRef(0),
    startingRef = useRef(false);
  useEffect(() => {
    let disposed = false;
    let timer: ReturnType<typeof setTimeout>;
    const controller = new AbortController();
    const poll = async () => {
      const version = generation.current;
      try {
        if (startingRef.current) return;
        const next = await dataSource.getSnapshot(controller.signal);
        if (!disposed && version === generation.current) {
          setSnapshot(next);
          setError(null);
        }
      } catch (e) {
        if (!disposed)
          setError(e instanceof Error ? e.message : "Could not load metrics");
      } finally {
        if (!disposed) timer = setTimeout(poll, 1000);
      }
    };
    void poll();
    return () => {
      disposed = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, []);
  const start = useCallback(async (config: RunConfig) => {
    generation.current++;
    startingRef.current = true;
    setStarting(true);
    try {
      setSnapshot(await dataSource.startRun(config));
      setError(null);
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not start run");
      return false;
    } finally {
      startingRef.current = false;
      setStarting(false);
    }
  }, []);
  return { snapshot, error, starting, start };
}
