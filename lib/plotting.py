import pandas as pd
import matplotlib.pyplot as plt

def create_plots():
    # Read the CSV file with the data
    data = pd.read_csv("../media/execution_and_error_data.csv", names=["EC2 Count", "Chunk Size", "Execution Time", "Error Rate"])

    # Plot Execution Time vs EC2 count and Chunk Size
    plt.figure(figsize=(12, 8))

    # Use pivot to structure data for plotting Execution Time vs EC2 Count and Chunk Size
    pivot_exec = data.pivot_table(values='Execution Time', index='EC2 Count', columns='Chunk Size')

    # Create a heatmap of execution time
    plt.title('Execution Time vs EC2 Count and Chunk Size')
    plt.xlabel('Chunk Size')
    plt.ylabel('EC2 Count')
    plt.imshow(pivot_exec, cmap='viridis', interpolation='nearest', aspect='auto')
    plt.colorbar(label='Execution Time (seconds)')
    plt.xticks(ticks=range(len(pivot_exec.columns)), labels=pivot_exec.columns)
    plt.yticks(ticks=range(len(pivot_exec.index)), labels=pivot_exec.index)

    # Save this plot
    plt.tight_layout()
    plt.savefig('../media/execution_time_vs_ec2_and_chunk_size.png')
    plt.close()

    # Plot Error Rate vs EC2 count and Chunk Size
    plt.figure(figsize=(12, 8))
    pivot_error = data.pivot_table(values='Error Rate', index='EC2 Count', columns='Chunk Size')

    # Create a heatmap of error rate
    plt.title('Error Rate vs EC2 Count and Chunk Size')
    plt.xlabel('Chunk Size')
    plt.ylabel('EC2 Count')
    plt.imshow(pivot_error, cmap='Reds', interpolation='nearest', aspect='auto')
    plt.colorbar(label='Error Rate (%)')
    plt.xticks(ticks=range(len(pivot_error.columns)), labels=pivot_error.columns)
    plt.yticks(ticks=range(len(pivot_error.index)), labels=pivot_error.index)

    # Save this plot
    plt.tight_layout()
    plt.savefig('../media/error_rate_vs_ec2_and_chunk_size.png')
    plt.close()

    # Plot Execution Time vs Chunk Size
    plt.figure(figsize=(8, 6))
    chunk_sizes = sorted(data['Chunk Size'].unique())
    execution_times_chunk_size = [
        data[data['Chunk Size'] == chunk_size]['Execution Time'].mean() for chunk_size in chunk_sizes
    ]
    plt.plot(chunk_sizes, execution_times_chunk_size, marker='o', color='b', linestyle='-', label='Execution Time')
    plt.title('Execution Time vs Chunk Size')
    plt.xlabel('Chunk Size')
    plt.ylabel('Execution Time (seconds)')
    plt.legend()

    # Save this plot
    plt.tight_layout()
    plt.savefig('../media/execution_time_vs_chunk_size.png')
    plt.close()

    # Plot Execution Time vs EC2 Count
    plt.figure(figsize=(8, 6))
    ec2_counts = sorted(data['EC2 Count'].unique())
    execution_times_ec2 = [
        data[data['EC2 Count'] == ec2_count]['Execution Time'].mean() for ec2_count in ec2_counts
    ]
    plt.plot(ec2_counts, execution_times_ec2, marker='o', color='g', linestyle='-', label='Execution Time')
    plt.title('Execution Time vs EC2 Count')
    plt.xlabel('EC2 Count')
    plt.ylabel('Execution Time (seconds)')
    plt.legend()

    # Save this plot
    plt.tight_layout()
    plt.savefig('../media/execution_time_vs_ec2_count.png')
    plt.close()

if __name__ == "__main__":
    create_plots()
