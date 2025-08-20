import argparse

def main():
    """
    Main function to parse command-line arguments for the pipeline.
    """
    parser = argparse.ArgumentParser(description="Data processing pipeline.")

    parser.add_argument(
        "--src",
        type=str,
        required=True,
        help="Source directory for the raw data."
    )

    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output directory for the processed data."
    )

    args = parser.parse_args()

    print(f"Source directory: {args.src}")
    print(f"Output directory: {args.out}")

if __name__ == "__main__":
    main()
