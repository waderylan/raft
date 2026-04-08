#!/usr/bin/env python3
"""
Parse result.txt and generate a latency-throughput plot.
Usage: python3 lat-tput.py [--input result.txt] [--output lat-tput.png]
"""

import argparse
import matplotlib.pyplot as plt

def parse_results(filepath):
    data = {
        "clientCount": [],
        "latAvg":      [],
        "latP50":      [],
        "latP90":      [],
        "latP99":      [],
        "throughput":  [],
    }

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            # Skip header lines
            if line.startswith("#") or line.startswith("-") or not line:
                continue
            parts = line.split()
            if len(parts) != 6:
                continue
            try:
                data["clientCount"].append(int(parts[0]))
                data["latAvg"].append(float(parts[1]))
                data["latP50"].append(float(parts[2]))
                data["latP90"].append(float(parts[3]))
                data["latP99"].append(float(parts[4]))
                data["throughput"].append(float(parts[5]))
            except ValueError:
                continue

    return data


def plot(data, output_path):
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(data["throughput"], data["latAvg"],
            marker="o", label="latAvg")
    ax.plot(data["throughput"], data["latP50"],
            marker="s", label="latP50")
    ax.plot(data["throughput"], data["latP90"],
            marker="^", label="latP90")
    ax.plot(data["throughput"], data["latP99"],
            marker="D", label="latP99")

    # Annotate each point with client count
    for i, n in enumerate(data["clientCount"]):
        ax.annotate(
            f"n={n}",
            (data["throughput"][i], data["latP99"][i]),
            textcoords="offset points",
            xytext=(5, 5),
            fontsize=8,
            color="gray"
        )

    ax.set_xlabel("Throughput (ops/sec)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_title("Latency vs Throughput (3-replica KV cluster)", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"Plot saved to {output_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="result.txt",   help="Input results file")
    parser.add_argument("--output", default="lat-tput.png", help="Output plot file")
    args = parser.parse_args()

    data = parse_results(args.input)
    if not data["throughput"]:
        print("No data found in input file")
        return

    plot(data, args.output)


if __name__ == "__main__":
    main()