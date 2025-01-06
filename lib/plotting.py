import pandas as pd
import matplotlib.pyplot as plt

def create_plots():
    # Read the CSV file with the data
    data = pd.read_csv("../media/execution_and_error_data.csv", names=["EC2 Count", "Chunk Size", "Execution Time", "Error Rate"])

    # Plot Execution Time vs EC2 count and Chunk Size
    plt.figure(figsize=(12, 6))

    # Use pivot to structure data for plotting
    pivot_exec = data.pivot_table(values='Execution Time', index='EC2 Count', columns='Chunk Size')

    # Create a heatmap of execution time
    plt.subplot(1, 2, 1)
    plt.title('Execution Time vs EC2 Count and Chunk Size')
    plt.xlabel('Chunk Size')
    plt.ylabel('EC2 Count')
    plt.imshow(pivot_exec, cmap='viridis', interpolation='nearest', aspect='auto')
    plt.colorbar(label='Execution Time (seconds)')
    plt.xticks(ticks=range(len(pivot_exec.columns)), labels=pivot_exec.columns)
    plt.yticks(ticks=range(len(pivot_exec.index)), labels=pivot_exec.index)

    # Plot Error Rate vs EC2 count and Chunk Size
    pivot_error = data.pivot_table(values='Error Rate', index='EC2 Count', columns='Chunk Size')

    # Create a heatmap of error rate
    plt.subplot(1, 2, 2)
    plt.title('Error Rate vs EC2 Count and Chunk Size')
    plt.xlabel('Chunk Size')
    plt.ylabel('EC2 Count')
    plt.imshow(pivot_error, cmap='Reds', interpolation='nearest', aspect='auto')
    plt.colorbar(label='Error Rate (%)')
    plt.xticks(ticks=range(len(pivot_error.columns)), labels=pivot_error.columns)
    plt.yticks(ticks=range(len(pivot_error.index)), labels=pivot_error.index)

    # Save the plot as a PNG file
    plt.tight_layout()
    plt.savefig('../media/execution_and_error_plots.png')

    # Optionally, display the plot as well
    plt.show()

if __name__ == "__main__":
    create_plots()
