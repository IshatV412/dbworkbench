import psycopg2
import time
import random
import string
import matplotlib.pyplot as plt
import numpy as np
import yaml
from pathlib import Path

# Configuration
DATA_SIZES_KB = [1, 10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
ITERATIONS = 10
OUTPUT_DIR = Path(__file__).parent

# Load database configuration from config.yaml
config_path = Path(__file__).parent.parent.parent / 'config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

rds_config = config['AWS_CREDS']['RDS']
ssl_cert_path = Path(__file__).parent.parent.parent / rds_config['SSL_CERT']

DB_CONFIG = {
    'host': rds_config['HOST'],
    'port': int(rds_config['PORT']),
    'database': rds_config['DATABASE'],
    'user': rds_config['USER'],
    'password': rds_config['PASSWORD'],
    'sslmode': rds_config['SSL_MODE'],
    'sslrootcert': str(ssl_cert_path)
}

def generate_random_text(size_kb):
    """Generate random text data of specified size in KB"""
    size_bytes = size_kb * 1024
    # Generate random alphanumeric text
    return ''.join(random.choices(string.ascii_letters + string.digits + ' \n', k=size_bytes))

def setup_test_table(conn):
    """Create a test table for performance testing"""
    cur = conn.cursor()
    try:
        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS performance_test")
        # Create table with id and text data
        cur.execute("""
            CREATE TABLE performance_test (
                id SERIAL PRIMARY KEY,
                data_size_kb INTEGER,
                text_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("Test table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise
    finally:
        cur.close()

def cleanup_test_table(conn):
    """Drop the test table"""
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS performance_test")
        conn.commit()
        print("Test table cleaned up")
    except Exception as e:
        print(f"Error cleaning up table: {e}")
    finally:
        cur.close()

def test_write_performance(conn, size_kb, text_data):
    """Test write performance for a specific data size"""
    cur = conn.cursor()
    try:
        start_time = time.perf_counter()
        cur.execute(
            "INSERT INTO performance_test (data_size_kb, text_data) VALUES (%s, %s) RETURNING id",
            (size_kb, text_data)
        )
        row_id = cur.fetchone()[0]
        conn.commit()
        end_time = time.perf_counter()
        
        write_time = (end_time - start_time) * 1000  # Return time in milliseconds
        return write_time, row_id
    except Exception as e:
        print(f"Error writing data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def test_read_performance(conn, row_id):
    """Test read performance for a specific row"""
    cur = conn.cursor()
    try:
        start_time = time.perf_counter()
        cur.execute("SELECT text_data FROM performance_test WHERE id = %s", (row_id,))
        data = cur.fetchone()
        end_time = time.perf_counter()
        
        read_time = (end_time - start_time) * 1000  # Return time in milliseconds
        return read_time
    except Exception as e:
        print(f"Error reading data: {e}")
        raise
    finally:
        cur.close()

def run_performance_tests():
    """Run all performance tests and collect data"""
    write_results = {size: [] for size in DATA_SIZES_KB}
    read_results = {size: [] for size in DATA_SIZES_KB}
    
    # Connect to database
    print("Connecting to RDS database...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected successfully!")
    
    try:
        # Setup test table
        setup_test_table(conn)
        
        print("\nRunning RDS performance tests...")
        print(f"Data sizes: {DATA_SIZES_KB} KB")
        print(f"Iterations per size: {ITERATIONS}")
        print("-" * 80)
        
        # Pre-generate test data to avoid including generation time in measurements
        print("\nGenerating test data...")
        test_data = {size: [generate_random_text(size) for _ in range(ITERATIONS)] 
                     for size in DATA_SIZES_KB}
        print("Test data generated")
        
        for iteration in range(1, ITERATIONS + 1):
            print(f"\nIteration {iteration}/{ITERATIONS}")
            for size_kb in DATA_SIZES_KB:
                text_data = test_data[size_kb][iteration - 1]
                
                # Test write
                write_time, row_id = test_write_performance(conn, size_kb, text_data)
                write_results[size_kb].append(write_time)
                
                # Test read
                read_time = test_read_performance(conn, row_id)
                read_results[size_kb].append(read_time)
                
                print(f"  {size_kb:4d} KB: Write={write_time:.3f} ms, Read={read_time:.3f} ms")
        
        # Cleanup
        cleanup_test_table(conn)
        
    finally:
        conn.close()
        print("\nDatabase connection closed")
    
    return write_results, read_results

def plot_results(write_results, read_results):
    """Generate plots for the performance test results"""
    sizes = DATA_SIZES_KB
    
    # Calculate statistics for writes
    write_mean = [np.mean(write_results[size]) for size in sizes]
    write_std = [np.std(write_results[size]) for size in sizes]
    write_min = [np.min(write_results[size]) for size in sizes]
    write_max = [np.max(write_results[size]) for size in sizes]
    
    # Calculate statistics for reads
    read_mean = [np.mean(read_results[size]) for size in sizes]
    read_std = [np.std(read_results[size]) for size in sizes]
    read_min = [np.min(read_results[size]) for size in sizes]
    read_max = [np.max(read_results[size]) for size in sizes]
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('RDS Write/Read Performance Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Mean write and read times with error bars
    ax1 = axes[0, 0]
    ax1.errorbar(sizes, write_mean, yerr=write_std, fmt='o-', capsize=5, 
                 linewidth=2, markersize=8, color='steelblue', label='Write')
    ax1.errorbar(sizes, read_mean, yerr=read_std, fmt='s-', capsize=5, 
                 linewidth=2, markersize=8, color='coral', label='Read')
    ax1.set_xlabel('Data Size (KB)', fontsize=12)
    ax1.set_ylabel('Latency (ms)', fontsize=12)
    ax1.set_title('Mean Latency with Standard Deviation', fontsize=13)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Box plot comparison
    ax2 = axes[0, 1]
    positions = np.arange(len(sizes))
    width = 0.35
    
    # Create side-by-side box plots
    write_data = [write_results[size] for size in sizes]
    read_data = [read_results[size] for size in sizes]
    
    bp1 = ax2.boxplot(write_data, positions=positions - width/2, widths=width*0.8,
                      patch_artist=True, showfliers=False)
    bp2 = ax2.boxplot(read_data, positions=positions + width/2, widths=width*0.8,
                      patch_artist=True, showfliers=False)
    
    for patch in bp1['boxes']:
        patch.set_facecolor('lightblue')
    for patch in bp2['boxes']:
        patch.set_facecolor('lightcoral')
    
    ax2.set_xticks(positions)
    ax2.set_xticklabels(sizes)
    ax2.set_xlabel('Data Size (KB)', fontsize=12)
    ax2.set_ylabel('Latency (ms)', fontsize=12)
    ax2.set_title('Distribution of Latencies (Box Plot)', fontsize=13)
    ax2.legend([bp1["boxes"][0], bp2["boxes"][0]], ['Write', 'Read'])
    ax2.grid(True, alpha=0.3, axis='y')
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 3: All iterations for writes
    ax3 = axes[1, 0]
    for i in range(ITERATIONS):
        iteration_times = [write_results[size][i] for size in sizes]
        ax3.plot(sizes, iteration_times, 'o-', alpha=0.3, linewidth=1, color='steelblue')
    ax3.plot(sizes, write_mean, 'ro-', linewidth=3, markersize=10, 
             label='Mean', zorder=10)
    ax3.set_xlabel('Data Size (KB)', fontsize=12)
    ax3.set_ylabel('Write Latency (ms)', fontsize=12)
    ax3.set_title('Write Operations - All Iterations', fontsize=13)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: All iterations for reads
    ax4 = axes[1, 1]
    for i in range(ITERATIONS):
        iteration_times = [read_results[size][i] for size in sizes]
        ax4.plot(sizes, iteration_times, 's-', alpha=0.3, linewidth=1, color='coral')
    ax4.plot(sizes, read_mean, 'go-', linewidth=3, markersize=10, 
             label='Mean', zorder=10)
    ax4.set_xlabel('Data Size (KB)', fontsize=12)
    ax4.set_ylabel('Read Latency (ms)', fontsize=12)
    ax4.set_title('Read Operations - All Iterations', fontsize=13)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = OUTPUT_DIR / "rds_performance_results.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {plot_path}")
    
    plt.show()
    
    return write_mean, read_mean

def print_summary(write_results, read_results):
    """Print summary statistics"""
    print("\n" + "=" * 90)
    print("WRITE PERFORMANCE SUMMARY")
    print("=" * 90)
    print(f"{'Size (KB)':<12} {'Mean (ms)':<12} {'Std Dev':<12} {'Min (ms)':<12} {'Max (ms)':<12}")
    print("-" * 90)
    
    for size in DATA_SIZES_KB:
        times = write_results[size]
        mean_time = np.mean(times)
        std_time = np.std(times)
        min_time = np.min(times)
        max_time = np.max(times)
        print(f"{size:<12} {mean_time:<12.3f} {std_time:<12.3f} {min_time:<12.3f} {max_time:<12.3f}")
    
    print("\n" + "=" * 90)
    print("READ PERFORMANCE SUMMARY")
    print("=" * 90)
    print(f"{'Size (KB)':<12} {'Mean (ms)':<12} {'Std Dev':<12} {'Min (ms)':<12} {'Max (ms)':<12}")
    print("-" * 90)
    
    for size in DATA_SIZES_KB:
        times = read_results[size]
        mean_time = np.mean(times)
        std_time = np.std(times)
        min_time = np.min(times)
        max_time = np.max(times)
        print(f"{size:<12} {mean_time:<12.3f} {std_time:<12.3f} {min_time:<12.3f} {max_time:<12.3f}")
    
    # Calculate throughput (MB/s)
    print("\n" + "=" * 90)
    print("THROUGHPUT")
    print("=" * 90)
    print(f"{'Size (KB)':<12} {'Write Throughput (MB/s)':<30} {'Read Throughput (MB/s)':<30}")
    print("-" * 90)
    
    for size in DATA_SIZES_KB:
        write_mean_sec = np.mean(write_results[size]) / 1000  # Convert to seconds
        read_mean_sec = np.mean(read_results[size]) / 1000  # Convert to seconds
        size_mb = size / 1024  # Convert KB to MB
        write_throughput = size_mb / write_mean_sec if write_mean_sec > 0 else 0
        read_throughput = size_mb / read_mean_sec if read_mean_sec > 0 else 0
        print(f"{size:<12} {write_throughput:<30.2f} {read_throughput:<30.2f}")

if __name__ == "__main__":
    print("Starting RDS Write/Read Performance Test")
    print("=" * 90)
    
    try:
        # Run tests
        write_results, read_results = run_performance_tests()
        
        # Print summary
        print_summary(write_results, read_results)
        
        # Generate plots
        plot_results(write_results, read_results)
        
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        raise
