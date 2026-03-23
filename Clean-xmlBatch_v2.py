import os
import re
import shutil
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

# SOURCE FOLDERS
INPUT_FOLDERS = [
    # r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML",
    # r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML\_invalid_member_numbers"
    r"C:\Users\IT\Documents\omniscan_images"
]

# OUTPUT FOLDER
# OUTPUT_FOLDER = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\cleaned_xml"
OUTPUT_FOLDER = r"C:\Users\IT\Documents\cleaned_xml"

# Ensure output exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def clean_member_number(value):
    if value is None:
        return None
    # Keep only letters and numbers
    return re.sub(r'[^A-Za-z0-9]', '', value)

def process_file(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        member_node = root.find(".//JT_Member_Number")

        if member_node is None:
            return f"SKIPPED (no member): {file_path}"

        original_value = member_node.text
        cleaned_value = clean_member_number(original_value)

        if not cleaned_value:
            return f"SKIPPED (empty after clean): {file_path}"

        # Update XML
        member_node.text = cleaned_value

        # New filename
        new_filename = f"{cleaned_value}.xml"
        output_path = os.path.join(OUTPUT_FOLDER, new_filename)

        # Handle duplicates
        counter = 1
        while os.path.exists(output_path):
            output_path = os.path.join(OUTPUT_FOLDER, f"{cleaned_value}_{counter}.xml")
            counter += 1

        # Save cleaned XML
        tree.write(output_path, encoding="utf-8", xml_declaration=True)

        return f"OK: {file_path} -> {output_path}"

    except Exception as e:
        return f"ERROR: {file_path} -> {str(e)}"


def get_all_files():
    files = []
    for folder in INPUT_FOLDERS:
        for root, _, filenames in os.walk(folder):
            for f in filenames:
                if f.lower().endswith(".xml"):
                    files.append(os.path.join(root, f))
    return files


if __name__ == "__main__":
    all_files = get_all_files()
    print(f"Total files found: {len(all_files)}")

    # Use threads for I/O-heavy workload
    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(process_file, all_files))

    # Optional: log results
    with open("processing_log.txt", "w", encoding="utf-8") as log:
        for r in results:
            log.write(r + "\n")

    print("Processing complete.")