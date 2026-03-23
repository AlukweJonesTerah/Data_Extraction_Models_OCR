import os
import re
import shutil

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Original full XMLs with wrong JT_Member_Number
invalid_xml_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML\_invalid_member_numbers"

# Simple XMLs produced by Extract-NSSF-Data.py (the correct numbers)
extracted_xml_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Extracted_XMLs_gpu_v2"

# Where to write the patched + renamed output files (originals stay untouched)
output_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\cleaned_xml_gpu_v2"

# Set True to preview without writing anything. Set False to apply.
DRY_RUN = False

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------
full_tag_pattern      = re.compile(r'<JT_Member_Number>(.*?)</JT_Member_Number>', re.IGNORECASE)
special_char_pattern  = re.compile(r'[^a-zA-Z0-9]')

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
patched_count    = 0   # Correct number found → patched + renamed
no_extract_count = 0   # No matching extracted XML found
invalid_count    = 0   # Extracted number itself was invalid
skipped_count    = 0   # Other skips (missing file, unreadable, etc.)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_extracted_nssf(base_name):
    """
    Looks up the correct NSSF number from the simple extracted XML.
    Returns the cleaned number string, or None if not found / invalid.
    """
    extracted_path = os.path.join(extracted_xml_dir, base_name + ".xml")
    if not os.path.exists(extracted_path):
        return None
    try:
        with open(extracted_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
        match = full_tag_pattern.search(content)
        if match:
            return match.group(1).strip()
    except Exception as e:
        print(f"  [!] Could not read extracted XML for {base_name}: {e}")
    return None

def is_valid_nssf(value):
    """
    Strict validation — digits and at most one letter, no punctuation,
    length 8–12.
    """
    if not value:
        return False
    clean = value.strip().upper()
    if clean in ("UNREADABLE", "MISSING_TAGS", "BLANK_MEMBER", ""):
        return False
    if not re.match(r'^[A-Z0-9]+$', clean):   # no dots, spaces, slashes
        return False
    if len(clean) < 8 or len(clean) > 12:
        return False
    if sum(c.isalpha() for c in clean) > 1:
        return False
    return True

def unique_output_path(directory, filename):
    """Returns a path that doesn't collide with existing files (_2, _3 ...)."""
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(directory, filename)
    counter = 2
    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{base}_{counter}{ext}")
        counter += 1
    return candidate

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def patch_and_rename():
    global patched_count, no_extract_count, invalid_count, skipped_count

    os.makedirs(output_dir, exist_ok=True)

    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Starting patch-and-rename job...")
    print(f"  Invalid XMLs   : {invalid_xml_dir}")
    print(f"  Extracted XMLs : {extracted_xml_dir}")
    print(f"  Output         : {output_dir}")
    print(f"  Mode           : {'DRY RUN — nothing will be written' if DRY_RUN else 'LIVE — writing files now'}\n")

    xml_files = sorted([f for f in os.listdir(invalid_xml_dir) if f.lower().endswith('.xml')])

    if not xml_files:
        print("No XML files found in invalid_xml_dir.")
        return

    for xml_filename in xml_files:
        base_name = os.path.splitext(xml_filename)[0]   # e.g. INVALID_OC1JOB_DISI-CLERK-38_record144
        src_path  = os.path.join(invalid_xml_dir, xml_filename)

        # ── Step 1: get the correct NSSF from the extracted XML ──────────
        correct_raw = read_extracted_nssf(base_name)

        if correct_raw is None:
            print(f"  [?] NO EXTRACT  {xml_filename}  →  no matching extracted XML, skipping")
            no_extract_count += 1
            continue

        # ── Step 2: clean special characters from the extracted number ───
        correct_clean = special_char_pattern.sub('', correct_raw).upper()

        if not is_valid_nssf(correct_clean):
            print(f"  [~] INVALID     {xml_filename}  →  extracted value '{correct_raw}' is not a valid NSSF, skipping")
            invalid_count += 1
            continue

        # ── Step 3: read the original full XML and patch the tag ─────────
        try:
            with open(src_path, 'r', encoding='utf-8-sig') as f:
                original_content = f.read()
        except Exception as e:
            print(f"  [!] READ ERROR  {xml_filename}: {e}")
            skipped_count += 1
            continue

        tag_match = full_tag_pattern.search(original_content)
        if not tag_match:
            print(f"  [!] NO TAG      {xml_filename}  →  <JT_Member_Number> tag not found")
            skipped_count += 1
            continue

        old_number   = tag_match.group(1)
        new_content  = full_tag_pattern.sub(
            f'<JT_Member_Number>{correct_clean}</JT_Member_Number>',
            original_content,
            count=1
        )

        # ── Step 4: build output filename ────────────────────────────────
        new_filename = f"{correct_clean}.xml"
        new_path     = unique_output_path(output_dir, new_filename)
        display_name = os.path.basename(new_path)

        if DRY_RUN:
            print(f"  [DRY RUN]  {xml_filename}")
            print(f"             old JT_Member_Number : '{old_number}'")
            print(f"             new JT_Member_Number : '{correct_clean}'")
            print(f"             saved as             : {display_name}")
        else:
            with open(new_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            shutil.copystat(src_path, new_path)   # preserve timestamps
            print(f"  [✓] PATCHED  {xml_filename}  →  {display_name}  (was: '{old_number}'  now: '{correct_clean}')")

        patched_count += 1

    print(f"\n--- Job Complete {'(DRY RUN — nothing written)' if DRY_RUN else ''} ---")
    print(f"Patched & renamed : {patched_count}")
    print(f"No extracted XML  : {no_extract_count}  (run extraction first for these)")
    print(f"Invalid extracted : {invalid_count}  (route to manual review)")
    print(f"Other skips       : {skipped_count}")

    if DRY_RUN:
        print("\nSet DRY_RUN = False to apply.")

if __name__ == "__main__":
    patch_and_rename()