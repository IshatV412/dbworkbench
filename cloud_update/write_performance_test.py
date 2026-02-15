import os
import time
import random
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Configuration
FILE_SIZES_KB = [1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
ITERATIONS = 20
OUTPUT_DIR = Path(__file__).parent / "test_files"
OUTPUT_DIR.mkdir(exist_ok=True)

def generate_random_binary_data(size_kb):
    """Generate random binary data of specified size in KB"""
    size_bytes = size_kb * 1024
    return bytes(random.getrandbits(8) for _ in range(size_bytes))

def test_write_performance(size_kb):
    """Test write performance for a specific file size"""
    data = generate_random_binary_data(size_kb)
    filepath = OUTPUT_DIR / f"test_{size_kb}kb.bin"
    
    start_time = time.perf_counter()
    with open(filepath, 'wb') as f:
        f.write(data)
    end_time = time.perf_counter()
    
    # Clean up
    filepath.unlink()
    
    return (end_time - start_time) * 1000  # Return time in milliseconds

def run_performance_tests():
    """Run all performance tests and collect data"""
    results = {size: [] for size in FILE_SIZES_KB}
    
    print("Running write performance tests...")
    print(f"File sizes: {FILE_SIZES_KB} KB")
    print(f"Iterations per size: {ITERATIONS}")
    print("-" * 60)
    
    for iteration in range(1, ITERATIONS + 1):
        print(f"\nIteration {iteration}/{ITERATIONS}")
        for size_kb in FILE_SIZES_KB:
            write_time = test_write_performance(size_kb)
            results[size_kb].append(write_time)
            print(f"  {size_kb:4d} KB: {write_time:.3f} ms")
    
    return results

def plot_results(results):
    """Generate plots for the performance test results"""
    sizes = FILE_SIZES_KB
    
    # Calculate statistics
    mean_times = [np.mean(results[size]) for size in sizes]
    std_times = [np.std(results[size]) for size in sizes]
    min_times = [np.min(results[size]) for size in sizes]
    max_times = [np.max(results[size]) for size in sizes]
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('File Write Performance Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Mean write time with error bars
    ax1 = axes[0, 0]
    ax1.errorbar(sizes, mean_times, yerr=std_times, fmt='o-', capsize=5, 
                 linewidth=2, markersize=8, color='steelblue')
    ax1.set_xlabel('File Size (KB)', fontsize=12)
    ax1.set_ylabel('Write Time (ms)', fontsize=12)
    ax1.set_title('Mean Write Time with Standard Deviation', fontsize=13)
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Box plot
    ax2 = axes[0, 1]
    box_data = [results[size] for size in sizes]
    bp = ax2.boxplot(box_data, labels=sizes, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('lightblue')
    ax2.set_xlabel('File Size (KB)', fontsize=12)
    ax2.set_ylabel('Write Time (ms)', fontsize=12)
    ax2.set_title('Distribution of Write Times (Box Plot)', fontsize=13)
    ax2.grid(True, alpha=0.3, axis='y')
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 3: All iterations
    ax3 = axes[1, 0]
    for i in range(ITERATIONS):
        iteration_times = [results[size][i] for size in sizes]
        ax3.plot(sizes, iteration_times, 'o-', alpha=0.3, linewidth=1)
    ax3.plot(sizes, mean_times, 'ro-', linewidth=3, markersize=10, 
             label='Mean', zorder=10)
    ax3.set_xlabel('File Size (KB)', fontsize=12)
    ax3.set_ylabel('Write Time (ms)', fontsize=12)
    ax3.set_title('All Iterations (Individual + Mean)', fontsize=13)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Min/Max range
    ax4 = axes[1, 1]
    ax4.fill_between(sizes, min_times, max_times, alpha=0.3, color='lightcoral', 
                     label='Min-Max Range')
    ax4.plot(sizes, mean_times, 'ro-', linewidth=2, markersize=8, label='Mean')
    ax4.plot(sizes, min_times, 'g--', linewidth=1.5, label='Min')
    ax4.plot(sizes, max_times, 'b--', linewidth=1.5, label='Max')
    ax4.set_xlabel('File Size (KB)', fontsize=12)
    ax4.set_ylabel('Write Time (ms)', fontsize=12)
    ax4.set_title('Min, Max, and Mean Write Times', fontsize=13)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = OUTPUT_DIR.parent / "write_performance_results.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {plot_path}")
    
    plt.show()
    
    return mean_times, std_times

def print_summary(results):
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("PERFORMANCE SUMMARY")
    print("=" * 60)
    print(f"{'Size (KB)':<12} {'Mean (ms)':<12} {'Std Dev':<12} {'Min (ms)':<12} {'Max (ms)':<12}")
    print("-" * 60)
    
    for size in FILE_SIZES_KB:
        times = results[size]
        mean_time = np.mean(times)
        std_time = np.std(times)
        min_time = np.min(times)
        max_time = np.max(times)
        print(f"{size:<12} {mean_time:<12.3f} {std_time:<12.3f} {min_time:<12.3f} {max_time:<12.3f}")
    
    # Calculate throughput (MB/s)
    print("\n" + "=" * 60)
    print("WRITE THROUGHPUT")
    print("=" * 60)
    print(f"{'Size (KB)':<12} {'Mean Throughput (MB/s)':<25}")
    print("-" * 60)
    
    for size in FILE_SIZES_KB:
        mean_time_sec = np.mean(results[size]) / 1000  # Convert to seconds
        size_mb = size / 1024  # Convert KB to MB
        throughput = size_mb / mean_time_sec if mean_time_sec > 0 else 0
        print(f"{size:<12} {throughput:<25.2f}")

if __name__ == "__main__":
    print("Starting File Write Performance Test")
    print("=" * 60)
    
    # Run tests
    results = run_performance_tests()
    
    # Print summary
    print_summary(results)
    
    # Generate plots
    plot_results(results)
    
    # Cleanup test directory if empty
    try:
        OUTPUT_DIR.rmdir()
    except:
        pass
    
    print("\nTest completed!")
