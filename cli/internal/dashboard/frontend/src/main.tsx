import { MantineProvider } from "@mantine/core";
import { createRoot } from "react-dom/client";
import App from "./App";
import { theme } from "./theme";
import "@mantine/core/styles.layer.css";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <MantineProvider theme={theme} forceColorScheme="light">
    <App />
  </MantineProvider>,
);
