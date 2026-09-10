"""
Test the eBay item details Raw reader.
"""

from ingestion.utils.item_details_reader import read_enriched_item_ids


def main() -> None:
    item_ids = read_enriched_item_ids()

    print("=" * 80)
    print("eBay Item Details Reader Test")
    print("=" * 80)

    print(f"Unique enriched item IDs: {len(item_ids)}")
    print(
        f"Duplicate item IDs: "
        f"{item_ids['item_id'].duplicated().sum()}"
    )

    print()
    print("Sample item IDs:")

    print(
        item_ids.head(10).to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()