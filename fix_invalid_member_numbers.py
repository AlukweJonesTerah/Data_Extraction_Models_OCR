"""
fix_invalid_member_numbers.py
------------------------------
For each XML in:
  \\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML\_invalid_member_numbers\

1. Looks up the matching record in cert_reg_results.json using the same base
   filename (with .tif extension) as the lookup key.
2. Cleans the NSSF_Number  — removes every character that is NOT a letter or
   digit (dashes, spaces, slashes, etc.).  Letters (e.g. trailing "X") are
   kept unchanged.
3. Replaces <JT_Member_Number> in the XML with the cleaned NSSF_Number.
4. Creates:
      XML\{clean_member_number}\{clean_member_number}.xml
5. Finds every matching TIFF / TIF in:
      TIFF\_invalid_member_numbers\
   whose filename starts with the same base name as the XML file.
6. Creates:
      TIFF\{clean_member_number}\
   and copies + renames each image:
      - If exactly one TIFF  -> {clean_member_number}.tif
      - If multiple TIFFs    -> {clean_member_number}_1.tif, _2.tif, ...
      (files are sorted alphabetically to preserve page order)

Usage
-----
Run from any Windows machine that can reach the network share:

    python fix_invalid_member_numbers.py

Or override paths via CLI:

    python fix_invalid_member_numbers.py ^
        --json     "\\\\192.168.1.11\\d\\BCERT_Phase4_Omniscan\\DMS_Upload\\cert_reg_results.json" ^
        --xml-src  "\\\\192.168.1.11\\d\\BCERT_Phase4_Omniscan\\DMS_Upload\\XML\\_invalid_member_numbers" ^
        --xml-out  "\\\\192.168.1.11\\d\\BCERT_Phase4_Omniscan\\DMS_Upload\\XML" ^
        --tiff-src "\\\\192.168.1.11\\d\\BCERT_Phase4_Omniscan\\DMS_Upload\\TIFF\\_invalid_member_numbers" ^
        --tiff-out "\\\\192.168.1.11\\d\\BCERT_Phase4_Omniscan\\DMS_Upload\\TIFF"

Requirements
------------
Python 3.9+ (uses ET.indent for pretty-printing; remove that line for 3.8).
No third-party packages needed.
"""

import argparse
import json
import os
import re
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# ---------------------------------------------------------------------------
# DEFAULT PATHS  (edit here if you prefer not to use CLI arguments)
# ---------------------------------------------------------------------------
DEFAULT_JSON_PATH = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\cert_reg_results.json"
DEFAULT_XML_SRC   = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML\_invalid_member_numbers"
DEFAULT_XML_OUT   = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\fixed_invalid_member_numbersXML"
DEFAULT_TIFF_SRC  = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
DEFAULT_TIFF_OUT  = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\fixed_invalid_member_numbersTIFF"
# ---------------------------------------------------------------------------


def clean_member_number(raw: str) -> str:
    """Strip every character that is not a letter (A-Z, a-z) or digit (0-9)."""
    return re.sub(r"[^A-Za-z0-9]", "", raw)


def find_tiff_files(tiff_src: Path, base_stem: str) -> list:
    """
    Return all .tif/.tiff files inside tiff_src whose stem starts with
    base_stem (case-insensitive), sorted alphabetically.
    """
    matches = []
    base_lower = base_stem.lower()
    for f in tiff_src.iterdir():
        if f.is_file() and f.suffix.lower() in (".tif", ".tiff"):
            if f.stem.lower().startswith(base_lower):
                matches.append(f)
    return sorted(matches)


def update_xml_member_number(xml_path: Path, new_number: str):
    """
    Parse the XML, replace every <JT_Member_Number> element's text with
    new_number, return the updated XML as a UTF-8 string (with declaration).
    """
    tree = ET.parse(str(xml_path))
    root = tree.getroot()

    updated_count = 0
    for elem in root.iter("JT_Member_Number"):
        old_val = elem.text
        elem.text = new_number
        print(f"    <JT_Member_Number>: {old_val!r}  ->  {new_number!r}")
        updated_count += 1

    if updated_count == 0:
        print(f"    WARNING: <JT_Member_Number> tag not found in {xml_path.name}")

    # ET.indent available from Python 3.9
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass  # Python < 3.9 — skip pretty-printing

    xml_body = ET.tostring(root, encoding="unicode", xml_declaration=False)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_body


def process_record(xml_file, json_data, xml_out_root, tiff_src, tiff_out_root):
    """Process one XML file.  Returns a status dict."""
    result = {
        "xml_file": xml_file.name,
        "status": "ok",
        "notes": [],
    }

    # 1. Look up NSSF_Number ------------------------------------------------
    tif_key = xml_file.stem + ".tif"   # e.g. INVALID_OC1JOB_..._record144.tif
    record  = json_data.get(tif_key)

    if record is None:
        result["status"] = "SKIPPED"
        result["notes"].append(f"No JSON entry for key '{tif_key}'")
        return result

    raw_nssf = record.get("NSSF_Number", "")
    if not raw_nssf:
        result["status"] = "SKIPPED"
        result["notes"].append("NSSF_Number is empty in JSON")
        return result

    clean_num = clean_member_number(str(raw_nssf))
    if not clean_num:
        result["status"] = "SKIPPED"
        result["notes"].append(
            f"NSSF_Number {raw_nssf!r} produced empty string after cleaning"
        )
        return result

    result["nssf_raw"]   = raw_nssf
    result["nssf_clean"] = clean_num

    print(f"\n  [{xml_file.name}]")
    print(f"    NSSF raw={raw_nssf!r}  ->  clean={clean_num!r}")

    # 2. Update XML ---------------------------------------------------------
    try:
        updated_xml = update_xml_member_number(xml_file, clean_num)
    except ET.ParseError as exc:
        result["status"] = "ERROR"
        result["notes"].append(f"XML parse error: {exc}")
        return result

    # 3. Save updated XML flat into output folder ---------------------------
    xml_out_root.mkdir(parents=True, exist_ok=True)
    xml_dest = xml_out_root / f"{clean_num}.xml"
    xml_dest.write_text(updated_xml, encoding="utf-8")
    print(f"    XML saved  -> {xml_dest}")
    result["xml_dest"] = str(xml_dest)

    # 4. Find and rename TIFFs flat into output folder ----------------------
    tiff_files = find_tiff_files(tiff_src, xml_file.stem)

    if not tiff_files:
        result["notes"].append("No matching TIFF files found")
    else:
        tiff_out_root.mkdir(parents=True, exist_ok=True)
        result["tiff_dests"] = []

        if len(tiff_files) == 1:
            dest = tiff_out_root / f"{clean_num}.tif"
            shutil.copy2(str(tiff_files[0]), str(dest))
            print(f"    TIFF copied -> {dest}")
            result["tiff_dests"].append(str(dest))
        else:
            for idx, src_tiff in enumerate(tiff_files, start=1):
                dest = tiff_out_root / f"{clean_num}_{idx}{src_tiff.suffix.lower()}"
                shutil.copy2(str(src_tiff), str(dest))
                print(f"    TIFF copied -> {dest}")
                result["tiff_dests"].append(str(dest))

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Fix invalid NSSF member numbers in XML files and rename TIFFs."
    )
    parser.add_argument("--json",     default=DEFAULT_JSON_PATH,
                        help="Path to cert_reg_results.json")
    parser.add_argument("--xml-src",  default=DEFAULT_XML_SRC,
                        help="Source folder with invalid XMLs")
    parser.add_argument("--xml-out",  default=DEFAULT_XML_OUT,
                        help="Output parent folder for corrected XMLs")
    parser.add_argument("--tiff-src", default=DEFAULT_TIFF_SRC,
                        help="Source folder with invalid TIFFs")
    parser.add_argument("--tiff-out", default=DEFAULT_TIFF_OUT,
                        help="Output parent folder for renamed TIFFs")
    args = parser.parse_args()

    json_path = Path(args.json)
    xml_src   = Path(args.xml_src)
    xml_out   = Path(args.xml_out)
    tiff_src  = Path(args.tiff_src)
    tiff_out  = Path(args.tiff_out)

    # Validate input paths --------------------------------------------------
    for label, p in [("JSON",        json_path),
                     ("XML source",  xml_src),
                     ("TIFF source", tiff_src)]:
        if not p.exists():
            print(f"ERROR: {label} path does not exist:\n  {p}")
            sys.exit(1)

    # Load JSON -------------------------------------------------------------
    print(f"Loading JSON: {json_path}")
    with open(str(json_path), encoding="utf-8") as fh:
        json_data = json.load(fh)
    print(f"  {len(json_data)} records loaded.")

    # Process XML files -----------------------------------------------------
    xml_files = sorted(xml_src.glob("*.xml"))
    if not xml_files:
        print(f"\nNo XML files found in: {xml_src}")
        sys.exit(0)

    print(f"\nFound {len(xml_files)} XML file(s) to process.")
    print("-" * 60)

    summary = []
    for xml_file in xml_files:
        res = process_record(xml_file, json_data, xml_out, tiff_src, tiff_out)
        summary.append(res)

    # Summary ---------------------------------------------------------------
    ok_list      = [r for r in summary if r["status"] == "ok"]
    skipped_list = [r for r in summary if r["status"] == "SKIPPED"]
    error_list   = [r for r in summary if r["status"] == "ERROR"]

    print("\n" + "=" * 60)
    print(f"SUMMARY  ({len(summary)} file(s) processed)")
    print("=" * 60)
    print(f"  OK      : {len(ok_list)}")
    print(f"  Skipped : {len(skipped_list)}")
    print(f"  Errors  : {len(error_list)}")

    if skipped_list:
        print("\nSkipped:")
        for r in skipped_list:
            print(f"  {r['xml_file']}")
            for note in r["notes"]:
                print(f"    -> {note}")

    if error_list:
        print("\nErrors:")
        for r in error_list:
            print(f"  {r['xml_file']}")
            for note in r["notes"]:
                print(f"    -> {note}")

    print("\nDone.")


if __name__ == "__main__":
    main()