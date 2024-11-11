import os


def create_data_folders():
    base_folders = [
        "data/livestate",
        "data/statearchive",
        "data/schemaarchive",
        "data/liveschema",
    ]

    # Create base folders
    for folder in base_folders:
        os.makedirs(folder, exist_ok=True)
        print(f"Created base folder: {folder}")


if __name__ == "__main__":
    create_data_folders()
