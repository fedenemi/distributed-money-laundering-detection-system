import argparse
import csv
import random


def reservoir_sample_rows(reader, sample_size, rng):
    sample = []
    for idx, row in enumerate(reader):
        if idx < sample_size:
            sample.append(row)
            continue
        j = rng.randrange(idx + 1)
        if j < sample_size:
            sample[j] = row
    return sample


def main():
    parser = argparse.ArgumentParser(
        description="Randomly sample rows from a large CSV."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    parser.add_argument(
        "--size",
        type=int,
        required=True,
        help="Number of rows to sample (excluding header)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible sampling",
    )
    args = parser.parse_args()

    if args.size <= 0:
        raise ValueError("Sample size must be positive")

    rng = random.Random(args.seed)

    with open(args.input, newline="") as infile:
        reader = csv.reader(infile)
        header = next(reader, None)
        if header is None:
            raise ValueError("Input file is empty")
        sample = reservoir_sample_rows(reader, args.size, rng)

    with open(args.output, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(sample)


if __name__ == "__main__":
    main()
