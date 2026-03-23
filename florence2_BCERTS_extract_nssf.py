import torch, json, re, os, sys, pathlib, csv, time
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM

MODEL_PATH = r"C:\Users\IT\Downloads\florence2-nssf-ocr-final"
TASK_TOKEN = "<Extract_NSSF_Data>"

# ── Output files — written to the same folder as the input ───────────────────
# Override by passing --output path\to\folder as the last argument
OUTPUT_JSON = "nssf_results.json"
OUTPUT_CSV  = "nssf_results.csv"
PROGRESS    = "nssf_progress.json"   # tracks which files are done — safe to resume

# ── All field names that may appear in any NSSF document type ─────────────────
# Used as CSV column headers. Add any new fields you discover here.
CSV_FIELDS = [
    "source_file", "page",
    "Document_Type", "Member_Name", "NSSF_Number", "Institution",
    "Date_of_Birth", "Date_of_Registration", "Date_of_Issue",
    "District", "Division", "Location", "Sub_location", "Village",
    "Tribe", "Occupation", "Sex_or_Gender", "Nationality",
    "ID_Number", "Issue_Office", "Employer_Number", "Employer_Name",
    "Address", "Place_of_Birth", "MRZ_Line", "Full_Names",
    "Receipt_Number", "Account_Number", "serial_number", "Receipt_Date",
    "Period", "Reference_Number", "Letter_Date",
    "Sender_Name", "Sender_Title", "Sender_Office",
    "Recipient_Name", "Recipient_Office", "Subject",
    "FM_Number", "Deceased_Name", "Date_of_Death",
    "Next_of_Kin", "Claimant_Name", "Bank_Name", "Bank_Branch",
    "Mobile_Phone", "Certificate_Number", "County", "Sub_County",
    "Type_of_Benefit_Paid", "Payroll_No", "NSSF_Officer",
    "Table",                # serialised as JSON string in CSV
    "_table_parse_failed",  # flag when Table block was too malformed to parse
    "_error",               # set if the page threw an exception
]


# ── Fix vision_config if needed (can get corrupted during LoRA merge) ─────────
cfg_path = os.path.join(MODEL_PATH, 'config.json')
if os.path.exists(cfg_path):
    with open(cfg_path) as f:
        cfg = json.load(f)
    if cfg.get('vision_config', {}).get('model_type') != 'davit':
        cfg['vision_config']['model_type'] = 'davit'
        with open(cfg_path, 'w') as f:
            json.dump(cfg, f, indent=2)
        print("⚠  Fixed vision_config.model_type → davit")


# ── Load model ────────────────────────────────────────────────────────────────
print("Loading model… (first run takes ~60 seconds)")
processor = AutoProcessor.from_pretrained(MODEL_PATH, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    trust_remote_code=True,
    torch_dtype=torch.float32,   # CPU requires fp32
).eval()
print("✅ Model ready\n")


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


# ── Extract from a single PIL image ──────────────────────────────────────────
def extract_image(pil_image):
    inputs = processor(text=TASK_TOKEN, images=pil_image, return_tensors="pt")
    with torch.no_grad():
        output = model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            max_new_tokens=640,
            num_beams=1,    # beams=1 on CPU — fastest setting
        )
    raw = processor.batch_decode(output, skip_special_tokens=True)[0]
    return robust_parse(raw)


# ── CSV helpers ───────────────────────────────────────────────────────────────
def result_to_csv_row(filename, page_num, result):
    """Flatten a result dict into a CSV row using CSV_FIELDS as columns."""
    row = {"source_file": filename, "page": page_num}
    for field in CSV_FIELDS[2:]:   # skip source_file and page
        val = result.get(field, "")
        # Serialise nested values (Table arrays/dicts) as JSON strings
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        row[field] = val
    return row

def append_csv_rows(csv_path, rows, write_header=False):
    mode = 'w' if write_header else 'a'
    with open(csv_path, mode, newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ── Process one file (handles multi-page TIF) ─────────────────────────────────
def process_file(file_path, csv_path, write_csv_header=False):
    name = pathlib.Path(file_path).name
    print(f"\n{'='*60}")
    print(f"FILE: {name}")
    print('='*60)

    img = Image.open(file_path)
    n_pages = getattr(img, 'n_frames', 1)
    file_results = []
    csv_rows = []

    for page in range(n_pages):
        img.seek(page)
        frame = img.convert('RGB')
        label = f"Page {page+1}/{n_pages}" if n_pages > 1 else "Single page"
        print(f"\n  [{label}] Processing…", end=" ", flush=True)
        t0 = time.time()

        try:
            result = extract_image(frame)
            elapsed = time.time() - t0
            file_results.append(result)
            print(f"✓  ({elapsed:.1f}s)")
            # Print result immediately to terminal
            print(json.dumps(result, indent=4, ensure_ascii=False))
            csv_rows.append(result_to_csv_row(name, page + 1, result))
        except Exception as e:
            elapsed = time.time() - t0
            err = {"_error": str(e)}
            file_results.append(err)
            print(f"✗ {e}  ({elapsed:.1f}s)")
            csv_rows.append(result_to_csv_row(name, page + 1, err))

    # Write this file's rows to CSV immediately
    if csv_rows:
        append_csv_rows(csv_path, csv_rows, write_header=write_csv_header)

    return file_results


# ── Process a folder (with resume support) ────────────────────────────────────
def process_folder(folder_path, output_dir=None):
    if output_dir is None:
        output_dir = folder_path

    json_path     = os.path.join(output_dir, OUTPUT_JSON)
    csv_path      = os.path.join(output_dir, OUTPUT_CSV)
    progress_path = os.path.join(output_dir, PROGRESS)

    exts = ("*.tif", "*.tiff", "*.jpg", "*.jpeg", "*.png")
    files = []
    for ext in exts:
        files.extend(pathlib.Path(folder_path).glob(ext))
    files = sorted(files)

    if not files:
        print(f"No images found in {folder_path}")
        return

    print(f"Found {len(files)} file(s) in {folder_path}")
    print(f"Results  → {json_path}")
    print(f"CSV      → {csv_path}")

    # Load existing progress (resume support)
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            done_files = set(json.load(f))
        print(f"Resuming — {len(done_files)} file(s) already done, skipping them")
    else:
        done_files = set()

    # Load existing JSON results
    if os.path.exists(json_path):
        with open(json_path, encoding='utf-8') as f:
            all_results = json.load(f)
    else:
        all_results = {}

    # Write CSV header only if starting fresh
    write_header = not os.path.exists(csv_path)

    pending = [f for f in files if f.name not in done_files]
    print(f"Pending  — {len(pending)} file(s) to process\n")

    for i, file_path in enumerate(pending):
        print(f"\n[{len(done_files)+i+1}/{len(files)}]", end="")
        pages = process_file(str(file_path), csv_path,
                             write_csv_header=(write_header and i == 0))

        # Store result
        all_results[file_path.name] = pages if len(pages) > 1 else pages[0]
        done_files.add(file_path.name)

        # Save JSON and progress after every file ─── safe to Ctrl+C
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        with open(progress_path, 'w') as f:
            json.dump(list(done_files), f)

    print(f"\n\n{'='*60}")
    print(f"✅ All done — {len(files)} file(s) processed")
    print(f"   JSON    → {json_path}")
    print(f"   CSV     → {csv_path}  (open directly in Excel)")
    print('='*60)


# ── Entry point ───────────────────────────────────────────────────────────────
INPUT_FOLDER  = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
OUTPUT_FOLDER = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload"

if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Default: process input folder, save outputs to DMS_Upload
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        process_folder(INPUT_FOLDER, output_dir=OUTPUT_FOLDER)

    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if os.path.isdir(arg):
            # Folder passed — save outputs alongside inputs
            process_folder(arg, output_dir=OUTPUT_FOLDER)
        else:
            # Single file — just print, no CSV
            process_file(arg, csv_path=os.devnull)

    elif len(sys.argv) == 3 and sys.argv[1] == "--output":
        # python florence2_extract_nssf.py --output \\server\share\other_folder
        os.makedirs(sys.argv[2], exist_ok=True)
        process_folder(INPUT_FOLDER, output_dir=sys.argv[2])

    else:
        print("Usage:")
        print("  python florence2_extract_nssf.py")
        print(r"      → process _invalid_member_numbers folder, save to \\192.168.1.11\d\...\DMS_Upload")
        print()
        print("  python florence2_extract_nssf.py path\\to\\folder")
        print(r"      → process folder, save outputs to \\192.168.1.11\d\...\DMS_Upload")
        print()
        print("  python florence2_extract_nssf.py path\\to\\file.tif")
        print("      → process single file, print to terminal only")
        print()
        print("  python florence2_extract_nssf.py --output \\\\server\\share\\other")
        print("      → process default input folder, save outputs to custom location")