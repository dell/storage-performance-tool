#!/bin/bash
# TUI Layout Testing Script for QA
# Generates layout debug files for different terminal sizes

echo "TUI Layout Testing Tool"
echo "======================="

# Build the debug tool
echo "Building debug tool..."
go build -mod=mod -o debug_layout debug_layout.go

if [ $? -ne 0 ]; then
    echo "❌ Failed to build debug tool"
    exit 1
fi

echo "✅ Debug tool built successfully"
echo ""

# Test different terminal sizes
sizes=(
    "120x25"  # Small terminal (tmux pane)
    "120x30"  # Medium terminal  
    "120x40"  # Large terminal
    "80x24"   # Standard 80-column terminal
    "160x50"  # Large widescreen
)

echo "Testing terminal sizes:"
for size in "${sizes[@]}"; do
    width=$(echo $size | cut -d'x' -f1)
    height=$(echo $size | cut -d'x' -f2)
    filename="layout_test_${size}.txt"
    
    echo "  📏 ${size} -> ${filename}"
    
    # Generate layout file
    ./debug_layout -width $width -height $height -output $filename -data > /dev/null 2>&1
    
    # Show quick summary
    ./debug_layout -calc -width $width -height $height | grep -E "(Terminal|OVERFLOW|Remaining)"
    echo ""
done

echo "Layout test files generated:"
ls -la layout_test_*.txt

echo ""
echo "🔍 To examine layouts:"
echo "   cat layout_test_120x25.txt  # Small terminal"
echo "   cat layout_test_120x40.txt  # Large terminal"
echo ""
echo "📊 Quick layout check:"
echo "   ./debug_layout -calc -width 120 -height 25"

# Cleanup
rm -f debug_layout