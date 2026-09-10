"""
Verify that successfully enriched items are excluded
from the pending enrichment set.
"""

from ingestion.utils.item_details_reader import read_enriched_item_ids
from ingestion.sources.ebay_enrichment_source import read_items_to_enrich


def main() -> None:
    """Verify that enriched items are no longer pending."""

    enriched_items = read_enriched_item_ids()

    pending_items = read_items_to_enrich()

    enriched_ids = set(
        enriched_items["item_id"]
    )

    pending_ids = set(
        pending_items["item_id"]
    )

    reappearing_ids = enriched_ids.intersection(
        pending_ids
    )

    print("=" * 80)
    print("Pending Enrichment Verification")
    print("=" * 80)

    print(
        f"Enriched item IDs : {len(enriched_ids)}"
    )

    print(
        f"Pending item IDs  : {len(pending_ids)}"
    )

    print(
        f"Reappearing IDs   : {len(reappearing_ids)}"
    )

    assert not reappearing_ids, (
        "Enriched items are still appearing "
        "in the pending enrichment set."
    )

    print()
    print("=" * 80)
    print("PENDING ITEM VERIFICATION PASSED")
    print("=" * 80)


if __name__ == "__main__":
    main()