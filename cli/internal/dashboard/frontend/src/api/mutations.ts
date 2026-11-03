import { useMutation, useQueryClient } from "@tanstack/react-query";
import { queryKeys } from "./queryKeys";
import {
  attachRun,
  detachRun,
  startRun,
  stopRun,
  verifyStorage,
} from "./requests";

export function useVerifyStorage() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: verifyStorage,
    onSuccess: () => client.invalidateQueries({ queryKey: queryKeys.state }),
  });
}

export function useStartRun() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: startRun,
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: queryKeys.state }),
        client.invalidateQueries({ queryKey: queryKeys.runs }),
        client.invalidateQueries({ queryKey: queryKeys.catalogRoot }),
      ]);
    },
  });
}

export function useAttachRun() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: attachRun,
    onSuccess: () => client.invalidateQueries({ queryKey: queryKeys.state }),
  });
}

export function useStopRun() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: stopRun,
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: queryKeys.state }),
        client.invalidateQueries({ queryKey: queryKeys.runs }),
        client.invalidateQueries({ queryKey: queryKeys.catalogRoot }),
      ]);
    },
  });
}

export function useDetachRun() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: detachRun,
    onSuccess: () => client.invalidateQueries({ queryKey: queryKeys.state }),
  });
}
