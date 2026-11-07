import "@mantine/core";
import type { DashboardTokens } from "./theme";

declare module "@mantine/core" {
  export interface MantineThemeOther {
    dashboard: DashboardTokens;
  }
}
