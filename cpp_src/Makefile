# Makefile for Scenario D

CXX = g++
CXXFLAGS = -std=c++11 -O3 -fopenmp -Wall -Wextra
TARGET = scenario_d

.PHONY: all clean test hpc

all: $(TARGET)

$(TARGET): scenario_d.cpp
	@echo "Compiling Scenario D..."
	$(CXX) $(CXXFLAGS) -o $(TARGET) scenario_d.cpp
	@echo "Build completed: ./$(TARGET)"
	@echo ""
	@echo "Usage:"
	@echo "  ./$(TARGET) <input.csv> <output_dir> <num_threads>"
	@echo "  Example: ./$(TARGET) data/subset_500.csv output/ 32"

clean:
	@echo "Cleaning build files..."
	rm -f $(TARGET)
	rm -rf output/*.csv output/*.json
	@echo "Clean completed"

test: $(TARGET)
	@echo "Running test with 4 threads..."
	mkdir -p output
	./$(TARGET) data/subset_500.csv output/ 4

hpc: $(TARGET)
	@echo "Running with 32 threads (HPC configuration)..."
	mkdir -p output
	./$(TARGET) data/subset_500.csv output/ 32

help:
	@echo "Scenario D - HPC Log Analysis"
	@echo ""
	@echo "Targets:"
	@echo "  all     - Build the program (default)"
	@echo "  clean   - Remove build files and outputs"
	@echo "  test    - Build and run with 4 threads"
	@echo "  hpc     - Build and run with 32 threads"
	@echo "  help    - Show this help message"
	@echo ""
	@echo "Direct execution:"
	@echo "  make && ./scenario_d data/subset_500.csv output/ 32"