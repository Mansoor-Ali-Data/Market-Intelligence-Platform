from utils.discovered_items_reader import read_discovered_item_ids
from utils.discovered_items_writer import write_discovered_items


def main() -> None:
    item_ids = read_discovered_item_ids()

    print(f"Discovered item IDs: {len(item_ids)}")

    write_discovered_items(item_ids)

    print("discovered_items Delta table written successfully.")


if __name__ == "__main__":
    main()