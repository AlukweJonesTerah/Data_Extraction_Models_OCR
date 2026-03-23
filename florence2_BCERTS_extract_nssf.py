import torch, json, re, os, sys, pathlib
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM

MODEL_PATH = r"C:\Users\IT\florence2-nssf-ocr-final"
TASK_TOKEN = "<Extract_NSSF_Data>"

# ── Fix vision_config if needed ───────────────────────────────────────────────
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
    torch_dtype=torch.float32,
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
            num_beams=1,
        )
    raw = processor.batch_decode(output, skip_special_tokens=True)[0]
    return robust_parse(raw)


# ── Process one file (handles multi-page TIF) ─────────────────────────────────
def process_file(file_path):
    name = pathlib.Path(file_path).name
    print(f"\n{'='*60}")
    print(f"FILE: {name}")
    print('='*60)

    img = Image.open(file_path)
    n_pages = getattr(img, 'n_frames', 1)
    file_results = []

    for page in range(n_pages):
        img.seek(page)
        frame = img.convert('RGB')

        label = f"Page {page+1}/{n_pages}" if n_pages > 1 else "Single page"
        print(f"\n  [{label}] Processing…", end=" ", flush=True)

        try:
            result = extract_image(frame)
            file_results.append(result)
            print("✓")
            # Print result immediately
            print(json.dumps(result, indent=4, ensure_ascii=False))
        except Exception as e:
            file_results.append({"_error": str(e)})
            print(f"✗ {e}")

    return file_results


# ── Process a folder ──────────────────────────────────────────────────────────
def process_folder(folder_path, output_json="results.json"):
    exts = ("*.tif", "*.tiff", "*.jpg", "*.jpeg", "*.png")
    files = []
    for ext in exts:
        files.extend(pathlib.Path(folder_path).glob(ext))
    files = sorted(files)

    if not files:
        print(f"No images found in {folder_path}")
        return

    print(f"Found {len(files)} file(s) in {folder_path}\n")
    all_results = {}

    for i, file_path in enumerate(files):
        print(f"\n[{i+1}/{len(files)}]", end="")
        pages = process_file(str(file_path))
        all_results[file_path.name] = pages if len(pages) > 1 else pages[0]

    # Save all results
    out_path = os.path.join(folder_path, output_json)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"\n\n{'='*60}")
    print(f"✅ All done — {len(files)} file(s) processed")
    print(f"   Results saved → {out_path}")
    print('='*60)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) == 1:
        process_folder(r"C:\Users\IT\nssf_docs")
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if os.path.isdir(arg):
            process_folder(arg)
        else:
            process_file(arg)
    else:
        print("Usage:")
        print("  python florence2_extract_nssf.py                       # process C:\\Users\\IT\\nssf_docs")
        print("  python florence2_extract_nssf.py path\\to\\folder        # process all files in folder")
        print("  python florence2_extract_nssf.py path\\to\\file.tif      # process single file")