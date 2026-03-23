import os
import random
import hashlib
import csv
import logging
import warnings
import numpy as np
from collections import defaultdict
from tqdm import tqdm
from PIL import Image

# ============================================================
# SUPPRESS ALL NOISY LOGS — must be set BEFORE model imports
# ============================================================
os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = "True"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"

warnings.filterwarnings("ignore")

for logger_name in [
    "ppocr", "paddle", "paddleocr", "paddlex",
    "transformers", "huggingface_hub",
    "httpx", "httpcore", "urllib3",
    "PIL", "filelock", "_client",
]:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# ============================================================
# IMPORT HEAVY LIBRARIES AFTER ENV VARS ARE SET
# ============================================================
from paddleocr import PaddleOCR
from transformers import TrOCRProcessor, VisionEncoderDecoderModel, logging as hf_logging

hf_logging.set_verbosity_error()

# ============================================================
# LOAD MODELS
# ============================================================
print("Loading PaddleOCR...")
paddle_ocr = PaddleOCR(use_textline_orientation=True, lang='en', enable_mkldnn=False)

print("Loading TrOCR...")
processor = TrOCRProcessor.from_pretrained(
    'microsoft/trocr-large-handwritten',
    local_files_only=True
)
model = VisionEncoderDecoderModel.from_pretrained(
    'microsoft/trocr-large-handwritten',
    local_files_only=True
)
model.config.tie_word_embeddings = False
print("Models loaded.\n")

# ============================================================
# CONSTANTS
# ============================================================
MAX_IMAGE_SIDE = 3500
TROCR_THUMB_SIZE = 384


def hash_file(filepath):
    """MD5 hash a file in chunks to detect duplicates."""
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            buf = f.read(65536)
            while buf:
                hasher.update(buf)
                buf = f.read(65536)
        return hasher.hexdigest()
    except Exception as e:
        print(f"  [hash_file] Failed on {filepath}: {e}")
        return None


def safe_open_image(file_path):
    """
    Open image safely, resizing large images to avoid PaddleOCR's
    max_side_limit warning (default 4000px).
    Returns (img_rgb, img_bgr_np) or (None, None) on failure.
    """
    try:
        with Image.open(file_path) as pil_img:
            img_rgb = pil_img.convert("RGB")

        w, h = img_rgb.size
        if max(w, h) > MAX_IMAGE_SIDE:
            scale = MAX_IMAGE_SIDE / max(w, h)
            new_size = (int(w * scale), int(h * scale))
            img_rgb = img_rgb.resize(new_size, Image.LANCZOS)

        img_bgr = np.array(img_rgb)[:, :, ::-1]
        return img_rgb, img_bgr

    except Exception as e:
        print(f"  [open_image] Skipping corrupt file: {file_path} | {e}")
        return None, None


def classify_document(file_path, min_confidence=0.6):
    """
    Classify a document image as:
      printed | handwritten | mixed | unknown | error

    PaddleOCR detects printed/typed text.
    TrOCR detects handwritten text.
    """
    printed_detected = False
    handwritten_detected = False

    img_rgb, img_bgr = safe_open_image(file_path)
    if img_rgb is None:
        return "error"

    # ---- PaddleOCR: Printed Text ----
    try:
        results = paddle_ocr.predict(img_bgr)
        if results:
            for page in results:
                texts = page.get("rec_texts", [])
                scores = page.get("rec_scores", [])
                for text, score in zip(texts, scores):
                    if score >= min_confidence and text.strip():
                        printed_detected = True
                        break
                if printed_detected:
                    break
    except Exception as e:
        print(f"  [PaddleOCR] Error on {os.path.basename(file_path)}: {e}")

    # ---- TrOCR: Handwritten Text ----
    try:
        thumb = img_rgb.copy()
        thumb.thumbnail((TROCR_THUMB_SIZE, TROCR_THUMB_SIZE))
        pixel_values = processor(images=thumb, return_tensors="pt").pixel_values
        generated_ids = model.generate(pixel_values)
        transcription = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
        if len(transcription.strip()) > 3:
            handwritten_detected = True
    except Exception as e:
        print(f"  [TrOCR] Error on {os.path.basename(file_path)}: {e}")

    if printed_detected and handwritten_detected:
        return "mixed"
    elif printed_detected:
        return "printed"
    elif handwritten_detected:
        return "handwritten"
    else:
        return "unknown"


def sample_documents(root_dir, total_samples=1000, seed=42, min_per_folder=10):
    """
    Proportionally sample images from all subfolders, classify them,
    deduplicate by hash, and write results to sampled_documents.csv.
    """
    random.seed(seed)
    folder_files = defaultdict(list)
    total_files = 0

    print(f"Scanning directory: {root_dir}")
    for root, dirs, files in os.walk(root_dir):
        for f in files:
            if f.lower().endswith(('.tif', '.tiff', '.png', '.jpg', '.jpeg', '.bmp')):
                folder_files[root].append(os.path.join(root, f))
                total_files += 1

    if total_files == 0:
        print("No images found. Check the target directory.")
        return []

    print(f"Found {total_files} images across {len(folder_files)} folders.")

    # Proportional candidate selection with 1.5x oversample buffer
    candidates = []
    for folder_path, files in folder_files.items():
        if not files:
            continue
        quota = max(min_per_folder, int((len(files) / total_files) * total_samples * 1.5))
        selected = random.sample(files, min(quota, len(files)))
        candidates.extend(selected)

    random.shuffle(candidates)
    print(f"Running classification on {len(candidates)} candidates...\n")

    sampled_data = []
    seen_hashes = set()
    category_counts = defaultdict(int)

    for f in tqdm(candidates, desc="Classifying", unit="img"):
        if len(sampled_data) >= total_samples:
            break

        h = hash_file(f)
        if not h or h in seen_hashes:
            continue

        category = classify_document(f)
        if category != "error":
            folder_name = os.path.basename(os.path.dirname(f))
            file_ext = os.path.splitext(f)[1].lower()
            sampled_data.append((folder_name, f, file_ext, category))
            seen_hashes.add(h)
            category_counts[category] += 1

    if len(sampled_data) > total_samples:
        sampled_data = random.sample(sampled_data, total_samples)

    output_file = "sampled_documents.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Folder", "FilePath", "Format", "Category"])
        writer.writerows(sampled_data)

    print(f"\n{'='*50}")
    print(f"Done. {len(sampled_data)} records saved to '{output_file}'")
    print("Category breakdown:")
    for cat, count in sorted(category_counts.items()):
        print(f"  {cat:<15}: {count}")
    print(f"{'='*50}\n")

    return sampled_data


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    target_dir = r'C:\\Users\\IT\\Desktop\\SAMPLE DATA\\CORRESPONDENCE'

    samples = sample_documents(
        root_dir=target_dir,
        total_samples=50,    # Increase to 1000 for full production run
        seed=42,
        min_per_folder=10
    )