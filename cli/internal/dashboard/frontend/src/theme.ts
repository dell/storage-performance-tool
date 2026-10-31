import { createTheme } from "@mantine/core";

export const theme = createTheme({
  fontFamily:
    'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  primaryColor: "spt",
  primaryShade: 6,
  colors: {
    spt: [
      "#eef4ff",
      "#eaf2fd",
      "#d4e4f8",
      "#78a9ee",
      "#347bd1",
      "#2273d6",
      "#1469c7",
      "#1766c1",
      "#1a65ba",
      "#125bb1",
    ],
  },
  fontSizes: { md: "14px" },
  radius: { md: "8px" },
  defaultRadius: "md",
  spacing: {
    xs: "8px",
    sm: "12px",
    md: "16px",
    lg: "20px",
    xl: "30px",
  },
  cursorType: "pointer",
});
