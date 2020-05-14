import matplotlib
import matplotlib.pyplot as plt
import numpy as np



def load_bench(job):
    tmp = []
    
    for dir in ['MRBenchmarks', 'SparkBenchmarks', 'HiveBenchmarks']:
        with open(f"{dir}/{job}.txt") as f:
            l = sorted(list(map(lambda e: (int(e[0]), int(float(e[1]))), map(lambda x: x.strip().split('\t'), f.readlines()))))
            tmp.append(l)
    
    keys = list(map(lambda x: x[0], tmp[0]))
    mr, spark, hive = list(map(lambda l: list(map(lambda e: e[1], l)), tmp))
    return keys, mr, spark, hive
    
    

def plot_job(job):

    keys, mr, spark, hive = load_bench(job)
    x = np.arange(len(keys))  # the label locations
    width = 0.25  # the width of the bars

    fig, ax = plt.subplots()
    rect_mr = ax.bar(x - width, mr, width, label='MapReduce')
    rect_spark = ax.bar(x, spark, width, label='Spark')
    rect_hive = ax.bar(x + width, hive, width, label='Hive')

    ax.set_ylabel('Time(s)')
    ax.set_xlabel('Rows processed')
    ax.set_title(f'Benchmarks for {job}')
    ax.set_xticks(x)
    ax.set_xticklabels(keys)
    ax.legend()

    def label(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    
    label(rect_mr)
    label(rect_spark)
    label(rect_hive)

    fig.tight_layout()
    fig.set_size_inches(18.5, 10.5)
    fig.savefig(f'bench_{job}.png', dpi=100)

    #plt.show()


for job in ['job1', 'job2', 'job3']:
    plot_job(job)