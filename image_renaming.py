import os
import re
import shutil

# --- Configuration (must match Extract-NSSF-Data.py) ---
source_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
xml_dir    = r'\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Extracted_XMLs_gpu_v2'
output_dir = r'\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Renamed_Images'

# Set to True to preview without copying. Set to False to actually copy.
DRY_RUN = False

# --- Counters ---
copied_count   = 0
skipped_count  = 0
conflict_count = 0

xml_pattern = re.compile(r'<JT_Member_Number>(.*?)</JT_Member_Number>', re.IGNORECASE)

def read_nssf_from_xml(xml_path):
    try:
        with open(xml_path, 'r', encoding='utf-8') as f:
            content = f.read()
        match = xml_pattern.search(content)
        if match:
            return match.group(1).strip()
    except Exception as e:
        print(f"  [!] Could not read {xml_path}: {e}")
    return None

def is_valid_nssf(value):
    """
    Strict NSSF validation:
    - 8 to 12 characters long
    - Only digits and at most one letter (no dots, spaces, slashes, etc.)
    - Not a placeholder value
    """
    if not value:
        return False
    clean = value.strip().upper()

    if clean in ("UNREADABLE", "MISSING_TAGS", ""):
        return False

    # Reject anything containing dots, spaces, slashes or other punctuation
    if not re.match(r'^[A-Z0-9]+$', clean):
        return False

    if len(clean) < 8 or len(clean) > 12:
        return False

    if sum(c.isalpha() for c in clean) > 1:
        return False

    return True

def rename_images():
    global copied_count, skipped_count, conflict_count

    # Always create output_dir (even in dry run so user can see it)
    os.makedirs(output_dir, exist_ok=True)

    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Starting rename job...")
    print(f"  Source (TIFFs) : {source_dir}")
    print(f"  XMLs           : {xml_dir}")
    print(f"  Output         : {output_dir}")
    print(f"  Mode           : {'DRY RUN — nothing will be copied' if DRY_RUN else 'LIVE — copying files now'}\n")

    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml')]

    if not xml_files:
        print("No XML files found. Run extraction first.")
        return

    for xml_filename in sorted(xml_files):
        base_name = os.path.splitext(xml_filename)[0]
        xml_path  = os.path.join(xml_dir, xml_filename)

        nssf_number = read_nssf_from_xml(xml_path)

        if not is_valid_nssf(nssf_number):
            print(f"  [~] SKIP     {base_name}  →  invalid value: '{nssf_number}'")
            skipped_count += 1
            continue

        # Find the matching image in source_dir
        original_path = None
        original_ext  = None
        for ext in ('.tif', '.tiff', '.jpg'):
            candidate = os.path.join(source_dir, base_name + ext)
            if os.path.exists(candidate):
                original_path = candidate
                original_ext  = ext
                break

        if not original_path:
            print(f"  [!] MISSING  {base_name}  →  no matching image found in source dir")
            skipped_count += 1
            continue

        # Build destination — handle duplicates by appending _2, _3, etc.
        new_filename = f"{nssf_number}{original_ext}"
        new_path     = os.path.join(output_dir, new_filename)

        if os.path.exists(new_path):
            counter = 2
            while True:
                new_filename = f"{nssf_number}_{counter}{original_ext}"
                new_path     = os.path.join(output_dir, new_filename)
                if not os.path.exists(new_path):
                    break
                counter += 1
            print(f"  [!] DUPLICATE  {base_name}{original_ext}  →  saving as '{new_filename}' (same NSSF number seen before)")
            conflict_count += 1

        if DRY_RUN:
            print(f"  [DRY RUN]    {base_name}{original_ext}  →  {new_filename}")
        else:
            shutil.copy2(original_path, new_path)
            print(f"  [✓] COPIED   {base_name}{original_ext}  →  {new_filename}")

        copied_count += 1

    print(f"\n--- Job Complete {'(DRY RUN — no files were copied)' if DRY_RUN else ''} ---")
    print(f"Copied:     {copied_count}")
    print(f"Skipped:    {skipped_count}  (invalid value or missing image)")
    print(f"Duplicates: {conflict_count}  (same NSSF number, renamed with _2, _3...)")

    if DRY_RUN:
        print("\nSet DRY_RUN = False to apply.")

if __name__ == "__main__":
    rename_images()