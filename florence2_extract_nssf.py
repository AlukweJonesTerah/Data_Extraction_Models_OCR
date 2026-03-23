import torch, json, re, os, sys, pathlib
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM

MODEL_PATH = r"c:\Users\IT\Downloads\florence2-nssf-ocr-final"
TASK_TOKEN = "<Extract_NSSF_Data>"

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
    torch_dtype=torch.float32,   # CPU requires fp32, not fp16
).eval()
print("✅ Model ready")


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


# ── Single image extraction ───────────────────────────────────────────────────
def extract(image_path):
    image = Image.open(image_path).convert("RGB")
    inputs = processor(text=TASK_TOKEN, images=image, return_tensors="pt")
    with torch.no_grad():
        output = model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            # attention_mask intentionally omitted — Florence-2 builds it internally
            max_new_tokens=640,
            num_beams=1,   # beams=1 on CPU — much faster, small accuracy cost
        )
    raw = processor.batch_decode(output, skip_special_tokens=True)[0]
    return robust_parse(raw)


# ── Folder batch processing ───────────────────────────────────────────────────
def process_folder(folder_path, output_json="results.json"):
    results = {}
    images = list(pathlib.Path(folder_path).glob("*.jpg")) + \
             list(pathlib.Path(folder_path).glob("*.png")) + \
             list(pathlib.Path(folder_path).glob("*.tif"))
    print(f"Found {len(images)} images in {folder_path}")
    for i, img_path in enumerate(images):
        print(f"[{i+1}/{len(images)}] {img_path.name}…", end=" ", flush=True)
        try:
            results[img_path.name] = extract(str(img_path))
            print("✓")
        except Exception as e:
            results[img_path.name] = {"_error": str(e)}
            print(f"✗ {e}")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nSaved → {output_json}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No argument — process the default folder
        process_folder(r"c:\Users\IT\Documents\nssf_dataset")

    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if os.path.isdir(arg):
            # Argument is a folder
            process_folder(arg)
        else:
            # Argument is a single image
            print(f"\nProcessing: {arg}")
            result = extract(arg)
            print(json.dumps(result, indent=2, ensure_ascii=False))

    else:
        print("Usage:")
        print("  python extract_nssf.py                              # process C:\\Users\\IT\\nssf_docs\\")
        print("  python extract_nssf.py path\\to\\folder              # process all images in folder")
        print("  python extract_nssf.py path\\to\\document.jpg        # process single image")