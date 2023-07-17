import os

import matplotlib.pyplot as plt
import numpy as np

execution_times = {
    "Map/Reduce - RDD API": {
        "Q1": 5.033608913421631,
        "Q2": 93.59469318389893,
        "Q3": 5.174684047698975,
        "Q4": 5.452530860900879,
        "Q5": 4.947165012359619
    },
    "Spark SQL on csv files": {
        "Q1": 12.712013006210327,
        "Q2": 36.51517105102539,
        "Q3": 8.97503113746643,
        "Q4": 15.23125696182251,
        "Q5": 7.966823101043701
    },
    "Spark SQL on parquet files": {
        "Q1": 7.862766981124878,
        "Q2": 6.805627107620239,
        "Q3": 7.029250144958496,
        "Q4": 12.496517181396484,
        "Q5": 7.869164943695068
    }
}

queries = ["Q1", "Q2", "Q3", "Q4", "Q5"]
x = np.arange(len(queries))
width = 0.25

fig, ax = plt.subplots(figsize=(10, 6))
rdd_api = ax.bar(x - width, [execution_times["Map/Reduce - RDD API"][q] for q in queries], width,
                 label="Map/Reduce - RDD API", color="#607D8B")
spark_csv = ax.bar(x, [execution_times["Spark SQL on csv files"][q] for q in queries], width,
                   label="Spark SQL on csv files", color="#FF5722")
spark_parquet = ax.bar(x + width, [execution_times["Spark SQL on parquet files"][q] for q in queries], width,
                       label="Spark SQL on parquet files", color="#2196F3")

ax.set_xlabel("Queries")
ax.set_ylabel("Execution Time (seconds)")
ax.set_title("Execution Times for Each Query")
ax.set_xticks(x)
ax.set_xticklabels(queries)
ax.legend()


# Add data labels
def add_labels(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f"{height:.2f}", xy=(rect.get_x() + rect.get_width() / 2, height), xytext=(0, 3),
                    textcoords="offset points", ha='center', va='bottom')


add_labels(rdd_api)
add_labels(spark_csv)
add_labels(spark_parquet)

# Remove spines
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# Adjust y-axis limits
ax.set_ylim(0, 100)

plt.tight_layout()

# Save the PNG file one directory higher
output_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
output_path = os.path.join(output_dir, "execution_times_chart.png")
plt.savefig(output_path)

plt.show()
