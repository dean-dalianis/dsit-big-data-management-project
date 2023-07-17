import json

import matplotlib.pyplot as plt
import numpy as np


def plot_execution_times(json_file_path, output_img_path):
    with open(json_file_path, 'r') as json_file:
        execution_times = json.load(json_file)

    join_types = list(execution_times.keys())
    execution_values = list(execution_times.values())

    x = np.arange(len(join_types))
    width = 0.35

    fig, ax = plt.subplots()
    bars = ax.bar(x, execution_values, width)

    ax.set_ylabel('Execution Time (sec)')
    ax.set_title('Execution Times for Different Join Types')
    ax.set_xticks(x)
    ax.set_xticklabels(join_types)
    ax.legend(bars, ['Execution Time'])

    def add_labels(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('%.4f' % height, xy=(rect.get_x() + rect.get_width() / 2, height), xytext=(0, 3),
                        textcoords='offset points', ha='center', va='bottom')

    add_labels(bars)

    plt.tight_layout()

    plt.savefig(output_img_path)


plot_execution_times('query_optimizer_execution_times_okeanos.json', '../output/query_optimizer_plot_okeanos.png')
