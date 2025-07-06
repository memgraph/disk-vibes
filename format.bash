#!/bin/bash -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if clang-format is installed
if ! command -v clang-format &> /dev/null; then
    echo "Error: clang-format is not installed"
    echo "Please install it using: brew install clang-format"
    exit 1
fi

# Check if black is installed
if ! command -v black &> /dev/null; then
    echo "Error: black is not installed"
    echo "Please install it using: pip install black"
    exit 1
fi

echo -e "${BLUE}Formatting C++ files...${NC}"
# Format all C++ files in src directory
find "${SCRIPT_DIR}/src" -type f \( -name "*.cpp" -o -name "*.hpp" \) -exec clang-format -i {} \;

echo -e "${BLUE}Formatting Python files...${NC}"
# Format all Python files, excluding ve3 and .venv directories
find "${SCRIPT_DIR}" -type f -name "*.py" -not -path "*/ve3/*" -not -path "*/.venv/*" -exec black {} \;

echo -e "${GREEN}Formatting complete!${NC}"