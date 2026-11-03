import { MantineProvider } from "@mantine/core";
import { QueryClientProvider } from "@tanstack/react-query";
import { createRoot } from "react-dom/client";
import App from "./App";
import { queryClient } from "./api/queryClient";
import { cssVariablesResolver, theme } from "./theme";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <MantineProvider
    theme={theme}
    cssVariablesResolver={cssVariablesResolver}
    forceColorScheme="light"
  >
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </MantineProvider>,
);
