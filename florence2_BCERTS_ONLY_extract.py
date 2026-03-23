"""
Florence-2 Extractor — Certificate of Registration (B)
=======================================================
Dedicated extractor for NSSF Certificate of Registration (B) documents.

What it does:
  - Reads multi-page TIF files from INPUT_FOLDER
  - Runs each page through the fine-tuned Florence-2-large model
  - Checks Document_Type — non-matching pages are skipped and flagged
  - Merges fields across all matching pages into one record per document
  - Saves to OUTPUT_FOLDER as JSON, CSV, and a skipped-files list
"""

import torch, json, re, os, sys, pathlib, csv, time
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM

# ── Paths ─────────────────────────────────────────────────────────────────────
MODEL_PATH    = r"C:\Users\IT\Downloads\florence2-nssf-ocr-final"
INPUT_FOLDER  = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
OUTPUT_FOLDER = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload"

TASK_TOKEN    = "<Extract_NSSF_Data>"
OUTPUT_JSON   = "cert_reg_results.json"
OUTPUT_CSV    = "cert_reg_results.csv"
SKIPPED_JSON  = "cert_reg_skipped.json"
PROGRESS_FILE = "cert_reg_progress.json"

# ── Document type matching ────────────────────────────────────────────────────
CERT_REG_VARIANTS = {
    "certificate of registration",
    "certificate of registration (b)",
    "cert of registration",
    "cert. of registration",
    "registration certificate",
    "certificate_of_registration",
}

def is_cert_reg(doc_type):
    return bool(doc_type) and doc_type.lower().strip() in CERT_REG_VARIANTS

# ── CSV columns ───────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "source_file",
    "NSSF_Number", "Employer_Number",
    "Member_Name", "Employer_Name", "Employer_Address",
    "Permanent_Address", "Date_of_First_Contribution", "Payroll_No",
    "District", "Division", "Location", "Sub_location",
    "ID_Number", "Date_of_Birth", "Sex_or_Gender",
    "Date_of_Registration", "NSSF_Officer", "Issuing_Officer",
    "serial_number", "Document_Type", "Institution",
    "source_pages_matched", "_table_parse_failed",
    "_no_cert_reg_page", "_error",
]

# Field aliases — model sometimes uses different key names for the same field
FIELD_ALIASES = {
    "Full_Names":                    "Member_Name",
    "Full Names":                    "Member_Name",
    "Name":                          "Member_Name",
    "Employer's_Number":             "Employer_Number",
    "Employer Number":               "Employer_Number",
    "ID_PP_No":                      "ID_Number",
    "ID/PP/No":                      "ID_Number",
    "ID_Passport_Number":            "ID_Number",
    "Gender":                        "Sex_or_Gender",
    "Date_of_first_contribution":    "Date_of_First_Contribution",
    "Date of first contribution":    "Date_of_First_Contribution",
    "Payroll Number":                "Payroll_No",
    "Sub_Location":                  "Sub_location",
    "Name_of_issuing_officer":       "Issuing_Officer",
    "Issuing Officer":               "Issuing_Officer",
}

def normalise_keys(result):
    out = {}
    for k, v in result.items():
        canonical = FIELD_ALIASES.get(k, k)
        if canonical not in out:
            out[canonical] = v
    return out

# ── Fix vision_config ─────────────────────────────────────────────────────────
cfg_path = os.path.join(MODEL_PATH, 'config.json')
if os.path.exists(cfg_path):
    with open(cfg_path) as f:
        cfg = json.load(f)
    if cfg.get('vision_config', {}).get('model_type') != 'davit':
        cfg['vision_config']['model_type'] = 'davit'
        with open(cfg_path, 'w') as f:
            json.dump(cfg, f, indent=2)
        print("Fixed vision_config.model_type -> davit")

# ── Load model ────────────────────────────────────────────────────────────────
print("Loading Florence-2 model...")
processor = AutoProcessor.from_pretrained(MODEL_PATH, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH, trust_remote_code=True, torch_dtype=torch.float32,
).eval()
print("Model ready\n")

# ── JSON parser ───────────────────────────────────────────────────────────────
def robust_parse(raw):
    raw = re.sub(r'<[^>]+>', '', raw).strip()
    if not raw:
        return {}
    def clean_keys(obj):
        if isinstance(obj, dict):
            return {k.strip(): clean_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_keys(i) for i in obj]
        return obj
    try:
        return clean_keys(json.loads(raw))
    except Exception:
        pass
    s2 = re.sub(r',\s*}', '}', raw)
    s2 = re.sub(r',\s*]', ']', s2)
    s2 = re.sub(r',\s*"[^"]*"\s*(?=[,}])', '', s2)
    try:
        return clean_keys(json.loads(s2))
    except Exception:
        pass
    last = s2.rfind('",')
    if last > 0:
        try:
            return clean_keys(json.loads(s2[:last+1] + '}'))
        except Exception:
            pass
    result = {}
    for m in re.finditer(r'"([^"]+)"\s*:\s*"([^"]*)"', raw):
        key, val = m.group(1).strip(), m.group(2)
        if key:
            result[key] = val
    if result:
        result['_table_parse_failed'] = True
        return result
    return {}

# ── Inference ─────────────────────────────────────────────────────────────────
def extract_image(pil_image):
    inputs = processor(text=TASK_TOKEN, images=pil_image, return_tensors="pt")
    with torch.no_grad():
        output = model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            max_new_tokens=640,
            num_beams=1,
        )
    raw = processor.batch_decode(output, skip_special_tokens=True)[0]
    return robust_parse(raw)

# ── Merge multi-page results ──────────────────────────────────────────────────
def merge_pages(page_results):
    merged = {}
    for page in page_results:
        for k, v in page.items():
            if k not in merged and v not in (None, "", []):
                merged[k] = v
    return merged

# ── Process one TIF file ──────────────────────────────────────────────────────
def process_file(file_path, csv_path, write_csv_header=False):
    name = pathlib.Path(file_path).name
    print(f"\n{'='*60}")
    print(f"FILE: {name}")
    print('='*60)

    img = Image.open(file_path)
    n_pages = getattr(img, 'n_frames', 1)
    cert_pages = []
    matched_page_nums = []

    for page in range(n_pages):
        img.seek(page)
        frame = img.convert('RGB')
        label = f"Page {page+1}/{n_pages}"
        print(f"\n  [{label}] Extracting...", end=" ", flush=True)
        t0 = time.time()
        try:
            raw_result = extract_image(frame)
            elapsed = time.time() - t0
            doc_type = raw_result.get('Document_Type', '')
            if is_cert_reg(doc_type):
                normalised = normalise_keys(raw_result)
                cert_pages.append(normalised)
                matched_page_nums.append(page + 1)
                print(f"OK Certificate of Registration  ({elapsed:.1f}s)")
                print(json.dumps(normalised, indent=4, ensure_ascii=False))
            else:
                print(f"SKIP  Document_Type: '{doc_type}'  ({elapsed:.1f}s)")
        except Exception as e:
            print(f"ERROR {e}  ({time.time()-t0:.1f}s)")

    if cert_pages:
        final = merge_pages(cert_pages)
        final['source_file']          = name
        final['source_pages_matched'] = ", ".join(str(p) for p in matched_page_nums)
        if len(cert_pages) > 1:
            print(f"\n  Merged {len(cert_pages)} matching pages into one record")
    else:
        final = {'source_file': name, '_no_cert_reg_page': True}
        print(f"\n  No Certificate of Registration page found")

    if csv_path != os.devnull:
        row = {f: "" for f in CSV_FIELDS}
        for k, v in final.items():
            if k in row:
                row[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
        mode = 'w' if write_csv_header else 'a'
        with open(csv_path, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
            if write_csv_header:
                writer.writeheader()
            writer.writerow(row)

    return final

# ── Process a folder ──────────────────────────────────────────────────────────
def process_folder(folder_path, output_dir=None):
    if output_dir is None:
        output_dir = folder_path
    os.makedirs(output_dir, exist_ok=True)

    json_path     = os.path.join(output_dir, OUTPUT_JSON)
    csv_path      = os.path.join(output_dir, OUTPUT_CSV)
    skipped_path  = os.path.join(output_dir, SKIPPED_JSON)
    progress_path = os.path.join(output_dir, PROGRESS_FILE)

    exts = ("*.tif", "*.tiff", "*.jpg", "*.jpeg", "*.png")
    files = []
    for ext in exts:
        files.extend(pathlib.Path(folder_path).glob(ext))
    files = sorted(files)

    if not files:
        print(f"No images found in {folder_path}")
        return

    print(f"Found {len(files)} file(s) in {folder_path}")
    print(f"Results  -> {json_path}")
    print(f"CSV      -> {csv_path}")

    done_files = set()
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            done_files = set(json.load(f))
        print(f"Resuming - {len(done_files)} file(s) already done, skipping")

    all_results = {}
    skipped = []
    if os.path.exists(json_path):
        with open(json_path, encoding='utf-8') as f:
            all_results = json.load(f)
    if os.path.exists(skipped_path):
        with open(skipped_path) as f:
            skipped = json.load(f)

    write_header = not os.path.exists(csv_path)
    pending = [f for f in files if f.name not in done_files]
    print(f"Pending  - {len(pending)} file(s) to process\n")

    matched_count = 0
    skipped_count = 0

    for i, file_path in enumerate(pending):
        print(f"\n[{len(done_files)+i+1}/{len(files)}]", end="")
        result = process_file(
            str(file_path), csv_path,
            write_csv_header=(write_header and i == 0)
        )
        fname = file_path.name
        if result.get('_no_cert_reg_page'):
            skipped.append(fname)
            skipped_count += 1
        else:
            all_results[fname] = result
            matched_count += 1

        done_files.add(fname)

        # Save after every file — safe to interrupt
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        with open(skipped_path, 'w') as f:
            json.dump(skipped, f, indent=2)
        with open(progress_path, 'w') as f:
            json.dump(list(done_files), f)

    print(f"\n\n{'='*60}")
    print(f"Complete - {len(files)} file(s) processed")
    print(f"  Certificate of Registration matched : {matched_count}")
    print(f"  Skipped (different document type)   : {skipped_count}")
    print(f"  JSON    -> {json_path}")
    print(f"  CSV     -> {csv_path}")
    print(f"  Skipped -> {skipped_path}")
    print('='*60)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) == 1:
        process_folder(INPUT_FOLDER, output_dir=OUTPUT_FOLDER)
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if os.path.isdir(arg):
            process_folder(arg, output_dir=OUTPUT_FOLDER)
        else:
            process_file(arg, csv_path=os.devnull)
    elif len(sys.argv) == 3 and sys.argv[1] == "--output":
        os.makedirs(sys.argv[2], exist_ok=True)
        process_folder(INPUT_FOLDER, output_dir=sys.argv[2])
    else:
        print("Usage:")
        print("  python florence2_extract_cert_reg.py")
        print("      process _invalid_member_numbers, save to DMS_Upload")
        print()
        print("  python florence2_extract_cert_reg.py path\\to\\folder")
        print("      process folder, save outputs to DMS_Upload")
        print()
        print("  python florence2_extract_cert_reg.py path\\to\\file.tif")
        print("      process single file, print to terminal only")
        print()
        print("  python florence2_extract_cert_reg.py --output \\\\server\\share\\other")
        print("      process default input, save to custom output folder")