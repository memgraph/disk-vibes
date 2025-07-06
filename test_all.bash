#!/bin/bash -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Building project...${NC}"

# Create and enter build directory
mkdir -p "${SCRIPT_DIR}/build"
rm -rf ${SCRIPT_DIR}/build/*
cd "${SCRIPT_DIR}/build"

# Configure with CMake
cmake -G Ninja "${SCRIPT_DIR}"

# Build all targets
ninja

echo -e "${GREEN}Build successful!${NC}"

# Run example graph
echo -e "${BLUE}Running example graph...${NC}"
"${SCRIPT_DIR}/build/example_graph"

# Run example graph
echo -e "${BLUE}Running example rocks...${NC}"
"${SCRIPT_DIR}/build/example_rocks"

# Run tests
echo -e "${BLUE}Running tests...${NC}"
ctest

# Run benchmark
echo -e "${BLUE}Running benchmark...${NC}"
"${SCRIPT_DIR}/build/benchmark" 10000 10000 "100,1000" "1,2"

# Setup Python virtual environment and install dependencies
echo -e "${BLUE}Setting up Python environment...${NC}"
VENV_DIR="${SCRIPT_DIR}/.venv"
if [ ! -d "${VENV_DIR}" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv "${VENV_DIR}"
    source "${VENV_DIR}/bin/activate"
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip install -r "${SCRIPT_DIR}/requirements.txt"
else
    source "${VENV_DIR}/bin/activate"
fi

# Plot benchmark results
echo -e "${BLUE}Plotting benchmark results...${NC}"
cd "${SCRIPT_DIR}"
python3 plot_benchmark.py

# Deactivate virtual environment
deactivate

echo -e "${GREEN}All binaries executed successfully!${NC}"
