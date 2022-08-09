"""Helper script to analyze performance."""
import argparse
import glob
import os.path
import pandas
import matplotlib.pyplot as plt


def plot(stub):
    """Produce comparison plots between PySpark and C functionalizer."""
    cdata = pandas.read_pickle(stub + "_old.pkl")
    pydata = pandas.read_pickle(stub + "_new.pkl")
    outfile = stub + "_kde.png"

    axs = cdata.plot.kde(
        subplots=True, layout=(2, 3), figsize=(14, 8), sharex=False, colormap="spring"
    )
    pydata.plot.kde(ax=axs.flatten()[:-1], subplots=True, alpha=0.8, colormap="winter")
    for ax in axs.flatten()[:-1]:
        lines, labels = ax.get_legend_handles_labels()
        ax.set_title(labels[0])
        ax.legend(lines, ["C", "Spark"], loc="best")
    print("Saving to", outfile)
    plt.suptitle(
        "{} (samles: {} [C], {} [Spark])".format(os.path.basename(stub), cdata.size, pydata.size)
    )
    plt.savefig(outfile)

    outfile = stub + "_hist.png"

    axs = cdata.plot.hist(
        subplots=True,
        density=True,
        layout=(2, 3),
        figsize=(14, 8),
        sharex=False,
        colormap="spring",
        bins=80,
    )
    pydata.plot.hist(
        ax=axs.flatten()[:-1],
        subplots=True,
        density=True,
        alpha=0.8,
        colormap="winter",
        bins=80,
    )
    for ax in axs.flatten()[:-1]:
        lines, labels = ax.get_legend_handles_labels()
        ax.set_yscale("log")
        ax.set_title(labels[0])
        ax.legend(lines, ["C", "Spark"], loc="best")
    print("Saving to", outfile)
    plt.suptitle(
        "{} (samles: {} [C], {} [Spark])".format(os.path.basename(stub), cdata.size, pydata.size)
    )
    plt.savefig(outfile)


parser = argparse.ArgumentParser()
parser.add_argument("dir")
opts = parser.parse_args()

for fn in glob.glob(os.path.join(opts.dir, "*_old.pkl")):
    plot(fn.replace("_old.pkl", ""))
