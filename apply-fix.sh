#!/bin/bash

# Script to apply the empty message handling fix
# This script applies the patch and verifies the changes

echo "🔧 Applying Empty Message Handling Fix..."
echo "=========================================="

# Check if patch file exists
if [ ! -f "burrowctl-empty-message-fix.patch" ]; then
    echo "❌ Error: Patch file not found!"
    echo "Please ensure 'burrowctl-empty-message-fix.patch' exists in the current directory."
    exit 1
fi

# Apply the patch
echo "📦 Applying patch..."
git apply burrowctl-empty-message-fix.patch

if [ $? -eq 0 ]; then
    echo "✅ Patch applied successfully!"
    echo ""
    echo "📋 Changes applied:"
    echo "- Multi-layer validation for empty messages"
    echo "- Enhanced error handling and logging"
    echo "- Worker pool message filtering"
    echo "- Heartbeat validation improvements"
    echo ""
    echo "🚀 The server should now handle empty messages gracefully."
    echo "📊 Monitor logs for reduced 'unexpected end of JSON input' errors."
else
    echo "❌ Error applying patch!"
    echo "Please check for conflicts and apply manually."
    exit 1
fi

echo ""
echo "📝 For detailed information, see: EMPTY_MESSAGE_FIX_DOCUMENTATION.md"
