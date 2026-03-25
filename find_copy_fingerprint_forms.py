r"""
Fingerprint Form Finder & Copier
=================================
Scans all 2026 folders in \\192.168.1.11\phase_v\BCERTS\RAW
Goes into each subfolder (batch -> image folders -> image files).
Runs Florence-2 on each page — if Document_Type is Fingerprint Form,
copies the entire file to \\192.168.1.11\d\Fullset samples
preserving the batch/image folder structure.

Saves a CSV scan log and resumes safely if interrupted.
"""

import torch, json, re, os, sys, csv, time, shutil
from datetime import datetime
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM

# ── Paths ─────────────────────────────────────────────────────────────────────
MODEL_PATH   = r"C:\Users\IT\Downloads\florence2-nssf-ocr-final"
SOURCE_ROOT  = r"\\192.168.1.11\phase_v\BCERTS\RAW"
DEST_FOLDER  = r"\\192.168.1.11\d\Fullset samples"
TASK_TOKEN   = "<Extract_NSSF_Data>"

LOG_CSV       = "fingerprint_scan_log.csv"
PROGRESS_FILE = "fingerprint_progress.json"

# ── Supported image extensions ────────────────────────────────────────────────
IMAGE_EXTENSIONS = {'.tif', '.tiff', '.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}

# ── Fingerprint Form variants ─────────────────────────────────────────────────
FINGERPRINT_VARIANTS = {
    "fingerprint form",
    "fingerprint",
    "finger print form",
    "fingerprint_form",
    "fingerprint form (b)",
    "fingerprint form b",
    "fingerprint card",
    "fp form",
    "finger print",
}

def is_fingerprint_form(doc_type):
    return bool(doc_type) and doc_type.lower().strip() in FINGERPRINT_VARIANTS

# ── CSV log columns ───────────────────────────────────────────────────────────
LOG_FIELDS = [
    "batch_folder", "image_folder", "filename", "page",
    "document_type", "is_fingerprint", "copied_to", "elapsed_s", "error",
]

def append_log(log_path, write_header, rows):
    mode = 'w' if write_header else 'a'
    with open(log_path, mode, newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction='ignore')
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ── Fix vision_config ─────────────────────────────────────────────────────────
def fix_vision_config():
    cfg_path = os.path.join(MODEL_PATH, 'config.json')
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            cfg = json.load(f)
        if cfg.get('vision_config', {}).get('model_type') != 'davit':
            cfg['vision_config']['model_type'] = 'davit'
            with open(cfg_path, 'w') as f:
                json.dump(cfg, f, indent=2)
            print("Fixed vision_config.model_type -> davit")


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


# ── Classify one page — only needs Document_Type ─────────────────────────────
def classify_page(pil_image, processor, model):
    inputs = processor(text=TASK_TOKEN, images=pil_image, return_tensors="pt")
    with torch.no_grad():
        output = model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            max_new_tokens=80,
            num_beams=1,
        )
    raw = processor.batch_decode(output, skip_special_tokens=True)[0]
    return robust_parse(raw).get('Document_Type', '')


# ── Discover 2026 batch folders ───────────────────────────────────────────────
def find_2026_batch_folders(source_root):
    folders = []
    skipped = 0
    try:
        for item in sorted(os.listdir(source_root)):
            full = os.path.join(source_root, item)
            try:
                if os.path.isdir(full):
                    ctime = datetime.fromtimestamp(os.path.getctime(full))
                    mtime = datetime.fromtimestamp(os.path.getmtime(full))
                    if ctime.year == 2026 or mtime.year == 2026:
                        folders.append(full)
            except OSError:
                skipped += 1
    except Exception as e:
        print(f"Error listing {source_root}: {e}")

    print(f"Found {len(folders)} batch folder(s) from 2026")
    if skipped:
        print(f"  ({skipped} folder(s) skipped due to access errors)")
    return folders


# ── Walk batch -> image subfolders -> image files ─────────────────────────────
def find_all_image_files(batch_folders):
    """
    Returns list of (batch_name, image_folder_name, file_path).
    Structure: SOURCE_ROOT / batch_folder / image_folder / file.*
    Supports: TIF, TIFF, JPG, JPEG, PNG, BMP, GIF, WEBP
    """
    all_files = []
    for batch_path in batch_folders:
        batch_name = os.path.basename(batch_path)
        try:
            for subfolder in sorted(os.listdir(batch_path)):
                sub_path = os.path.join(batch_path, subfolder)
                if not os.path.isdir(sub_path):
                    continue
                for fname in sorted(os.listdir(sub_path)):
                    ext = os.path.splitext(fname)[1].lower()
                    if ext in IMAGE_EXTENSIONS:
                        all_files.append((
                            batch_name,
                            subfolder,
                            os.path.join(sub_path, fname),
                        ))
        except OSError as e:
            print(f"  Skipping {batch_path}: {e}")
    return all_files


# ── Main scan ─────────────────────────────────────────────────────────────────
def run_scan(processor, model):
    os.makedirs(DEST_FOLDER, exist_ok=True)
    log_path      = os.path.join(DEST_FOLDER, LOG_CSV)
    progress_path = os.path.join(DEST_FOLDER, PROGRESS_FILE)

    # Resume support
    done_keys = set()
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            done_keys = set(json.load(f))
        print(f"Resuming - {len(done_keys)} page(s) already classified")

    write_header = not os.path.exists(log_path)

    # Discover
    print(f"\nScanning: {SOURCE_ROOT}")
    batch_folders = find_2026_batch_folders(SOURCE_ROOT)
    all_files     = find_all_image_files(batch_folders)
    print(f"Total image files to scan: {len(all_files)}\n")

    total_pages = 0
    fp_files    = 0
    copied      = 0
    errors      = 0

    for file_idx, (batch_name, img_folder, file_path) in enumerate(all_files):
        fname = os.path.basename(file_path)
        rel   = f"{batch_name}\\{img_folder}\\{fname}"

        try:
            img     = Image.open(file_path)
            n_pages = getattr(img, 'n_frames', 1)
        except Exception as e:
            print(f"[{file_idx+1}/{len(all_files)}] OPEN ERROR {rel}: {e}")
            append_log(log_path, write_header, [{
                "batch_folder": batch_name, "image_folder": img_folder,
                "filename": fname, "page": "-", "document_type": "",
                "is_fingerprint": False, "copied_to": "", "elapsed_s": 0,
                "error": str(e),
            }])
            write_header = False
            errors += 1
            continue

        file_is_fingerprint = False

        for page in range(n_pages):
            progress_key = f"{rel}::p{page}"
            if progress_key in done_keys:
                continue

            img.seek(page)
            frame = img.convert('RGB')
            label = f"p{page+1}/{n_pages}"
            print(f"[{file_idx+1}/{len(all_files)}] {rel} [{label}]...",
                  end=" ", flush=True)
            t0 = time.time()

            try:
                doc_type     = classify_page(frame, processor, model)
                elapsed      = round(time.time() - t0, 1)
                is_fp        = is_fingerprint_form(doc_type)
                total_pages += 1

                if is_fp:
                    file_is_fingerprint = True
                    print(f"FINGERPRINT FORM  ({elapsed}s)")
                else:
                    print(f"{doc_type or 'UNKNOWN'}  ({elapsed}s)")

                append_log(log_path, write_header, [{
                    "batch_folder":   batch_name,
                    "image_folder":   img_folder,
                    "filename":       fname,
                    "page":           page + 1,
                    "document_type":  doc_type,
                    "is_fingerprint": is_fp,
                    "copied_to":      "",
                    "elapsed_s":      elapsed,
                    "error":          "",
                }])
                write_header = False

            except Exception as e:
                elapsed = round(time.time() - t0, 1)
                print(f"ERROR {e}  ({elapsed}s)")
                append_log(log_path, write_header, [{
                    "batch_folder": batch_name, "image_folder": img_folder,
                    "filename": fname, "page": page+1, "document_type": "",
                    "is_fingerprint": False, "copied_to": "", "elapsed_s": elapsed,
                    "error": str(e),
                }])
                write_header = False
                errors += 1

            done_keys.add(progress_key)

        # Copy the whole file once if any page was a Fingerprint Form
        if file_is_fingerprint:
            fp_files += 1
            dest_subdir = os.path.join(DEST_FOLDER, batch_name, img_folder)
            os.makedirs(dest_subdir, exist_ok=True)
            dest_path = os.path.join(dest_subdir, fname)
            if not os.path.exists(dest_path):
                shutil.copy2(file_path, dest_path)
                copied += 1
                print(f"  -> COPIED to {dest_path}")
            else:
                print(f"  -> Already in destination, skipped")

        # Save progress after every file
        with open(progress_path, 'w') as f:
            json.dump(list(done_keys), f)

    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE")
    print(f"  Image files scanned     : {len(all_files)}")
    print(f"  Pages classified        : {total_pages}")
    print(f"  Fingerprint Form files  : {fp_files}")
    print(f"  Files copied            : {copied}")
    print(f"  Errors                  : {errors}")
    print(f"  Log CSV                 : {log_path}")
    print(f"  Destination             : {DEST_FOLDER}")
    print('='*60)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":

    if len(sys.argv) == 2 and sys.argv[1] == "--list-only":
        # Dry run: NO model loading — instant results
        batch_folders = find_2026_batch_folders(SOURCE_ROOT)
        all_files     = find_all_image_files(batch_folders)
        print(f"\nImage files that would be scanned: {len(all_files)}")
        for batch_name, img_folder, fpath in all_files[:30]:
            print(f"  {batch_name}\\{img_folder}\\{os.path.basename(fpath)}")
        if len(all_files) > 30:
            print(f"  ... and {len(all_files)-30} more")

    else:
        # Load model only when actually scanning
        fix_vision_config()
        print("Loading Florence-2 model...")
        processor = AutoProcessor.from_pretrained(MODEL_PATH, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_PATH, trust_remote_code=True, torch_dtype=torch.float32,
        ).eval()
        print("Model ready\n")
        run_scan(processor, model)