import json

import matplotlib.pyplot as plt
import numpy as np


def plot_execution_times(json_file_path, output_img_path):
    with open(json_file_path, 'r') as json_file:
        execution_times = json.load(json_file)

    queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
    x = np.arange(len(queries))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 6))
    rdd_api = ax.bar(x - width, [execution_times['Map/Reduce - RDD API'][q] for q in queries], width,
                     label='Map/Reduce - RDD API', color='#607D8B')
    spark_csv = ax.bar(x, [execution_times['Spark SQL on csv files'][q] for q in queries], width,
                       label='Spark SQL on csv files', color='#FF5722')
    spark_parquet = ax.bar(x + width, [execution_times['Spark SQL on parquet files'][q] for q in queries], width,
                           label='Spark SQL on parquet files', color='#2196F3')

    ax.set_xlabel('Queries')
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Execution Times for Each Query')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()

    def add_labels(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('%.2f' % height, xy=(rect.get_x() + rect.get_width() / 2, height), xytext=(0, 3),
                        textcoords='offset points', ha='center', va='bottom')

    add_labels(rdd_api)
    add_labels(spark_csv)
    add_labels(spark_parquet)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.set_ylim(0, 100)

    plt.tight_layout()

    plt.savefig(output_img_path)


plot_execution_times('spark_execution_times_hortonworks.json', '../output/rdd_df_plot_hortonworks.png')
plot_execution_times('spark_execution_times_okeanos.json', '../output/rdd_df_plot_okeanos.png')
