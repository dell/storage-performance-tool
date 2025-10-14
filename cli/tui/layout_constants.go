package tui

// Viewport heights define the fixed heights for different UI sections
const (
	// LiveViewHeight is the fixed height for the live view panel (including borders)
	// 1 header + 2 data rows + 2 borders
	LiveViewHeight = 5

	// ChartViewportHeight is the total height for chart viewport (including borders)
	// 8 content + 2 borders
	ChartViewportHeight = 10

	// ChartContentHeight is the height of chart content (excluding borders)
	// 1 header + 6 bars + 1 timestamp
	ChartContentHeight = 8

	// ProcessViewportMinHeight is the minimum height for process output viewport
	ProcessViewportMinHeight = 5

	// ProcessViewportMaxHeight is the maximum height for process output viewport
	ProcessViewportMaxHeight = 25
)

// Layout components define the sizes of various UI elements
const (
	// StatusLineHeight is the height of the status line at the bottom
	StatusLineHeight = 1

	// ExtraLayoutPadding is additional padding for layout calculations
	ExtraLayoutPadding = 2

	// PageScrollSize is the number of lines to scroll on page up/down
	PageScrollSize = 10

	// BorderWidth is the width of viewport borders
	BorderWidth = 2

	// ViewportPadding is the internal padding for viewports
	ViewportPadding = 4
)

// Layout calculations define derived values for common height calculations
const (
	// LayoutHeightWithCharts is the total fixed height when charts are visible
	// StatusLineHeight + ChartViewportHeight + LiveViewHeight + ExtraLayoutPadding
	LayoutHeightWithCharts = StatusLineHeight + ChartViewportHeight + LiveViewHeight + ExtraLayoutPadding // 1 + 10 + 5 + 2 = 18

	// LayoutHeightWithoutCharts is the total fixed height when charts are hidden
	// StatusLineHeight + LiveViewHeight + ExtraLayoutPadding
	LayoutHeightWithoutCharts = StatusLineHeight + LiveViewHeight + ExtraLayoutPadding // 1 + 5 + 2 = 8

	// ViewportContentPadding is the total padding that reduces content width
	// BorderWidth + ViewportPadding
	ViewportContentPadding = BorderWidth + ViewportPadding // 2 + 4 = 6
)
