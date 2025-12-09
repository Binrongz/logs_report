#SBATCH --job-name=scenario_d
#SBATCH -A m3930
#SBATCH --qos=debug
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --time=00:10:00
#SBATCH --output=logs/scenario_d_%j.out
#SBATCH --error=logs/scenario_d_%j.err

echo "========================================"
echo "Scenario D: HPC Log Analysis"
echo "========================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Start time: $(date)"
echo "========================================"

# Load required modules
echo ""
echo "Loading modules..."
module load gcc/11.2.0
module list

# Set OpenMP environment
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export OMP_PROC_BIND=true
export OMP_PLACES=cores

echo ""
echo "OpenMP Configuration:"
echo "  OMP_NUM_THREADS=$OMP_NUM_THREADS"
echo "  OMP_PROC_BIND=$OMP_PROC_BIND"
echo "  OMP_PLACES=$OMP_PLACES"

# Navigate to submission directory
cd $SLURM_SUBMIT_DIR

# Create output directories
mkdir -p output logs

# Compile
echo ""
echo "========================================"
echo "Compiling..."
echo "========================================"
make clean
make

# Check if compilation succeeded
if [ ! -f "./scenario_d" ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi

echo "Compilation successful"

# Run the program
echo ""
echo "========================================"
echo "Running Scenario D..."
echo "========================================"
time ./scenario_d subset_500.csv output/ $SLURM_CPUS_PER_TASK

# Check if execution succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "Execution completed successfully"
    echo "========================================"
else
    echo ""
    echo "========================================"
    echo "ERROR: Execution failed!"
    echo "========================================"
    exit 1
fi

# Display output files
echo ""
echo "Generated files:"
ls -lh output/

echo ""
echo "========================================"
echo "Job completed: $(date)"
echo "========================================"