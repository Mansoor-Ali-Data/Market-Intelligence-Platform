from ingestion.sources.ebay_enrichment_source import pending_items


def main() -> None:
    items = list(pending_items())

    print(f"Pending records returned: {len(items)}")

    if items:
        print("Sample:")
        for item in items[:5]:
            print(item)


if __name__ == "__main__":
    main()