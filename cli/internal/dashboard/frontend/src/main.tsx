import { MantineProvider } from "@mantine/core";
import { createRoot } from "react-dom/client";
import App from "./App";
import { cssVariablesResolver, theme } from "./theme";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <MantineProvider
    theme={theme}
    cssVariablesResolver={cssVariablesResolver}
    forceColorScheme="light"
  >
    <App />
  </MantineProvider>,
);
