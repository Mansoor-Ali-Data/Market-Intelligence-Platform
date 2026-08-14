import argparse
from datetime import date, datetime, timedelta, timezone

from pipelines.ebay_pipeline import run_pipeline


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Run eBay daily ingestion."
    )

    parser.add_argument(
        "--date",
        dest="extraction_date",
        type=str,
        help="UTC extraction date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:

    args = parse_args()

    if args.extraction_date:

        extraction_date = datetime.strptime(
            args.extraction_date,
            "%Y-%m-%d",
        ).date()

    else:

        # Default to previous UTC day.
        extraction_date = (
            datetime.now(timezone.utc).date()
            - timedelta(days=1)
        )

    print(
        f"Running eBay ingestion for "
        f"extraction_date={extraction_date}"
    )

    run_pipeline(extraction_date)


if __name__ == "__main__":
    main()