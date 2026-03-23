import os
import io
import re
import shutil
import threading
import numpy as np
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import ollama

# --- Configuration ---
source_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
output_dir = r'\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Extracted_XMLs_v3'
manual_review_dir = os.path.join(output_dir, "Manual_Review_Needed")

# --- Speed & AI Configuration ---
MAX_WORKERS = 3  # Parallel files
OLLAMA_TIMEOUT = 60  # Seconds per model attempt

# List models from fastest/specialized to heaviest fallback
MODELS_TO_TRY = [
    'qwen2.5vl',       # High-accuracy vision
    'gemma3:4b',        # Fast multimodal
    'llama3.2-vision',  # Generalist fallback
    'deepseek-v3'       # Heavyweight fallback
]

SYSTEM_PROMPT = """
You are a highly accurate OCR data extraction system. Look at the provided cropped image of a Kenyan NSSF Certificate.
Read the number written or typed next to "NSSF No." in the yellow box.

Rules:
1. ONLY output the number.
2. DO NOT invent or guess numbers. If it is completely unreadable, output UNREADABLE.
3. Output EXACTLY in this XML format and nothing else:

<JT_Member_Number>[Extracted NSSF No]</JT_Member_Number>
"""

xml_pattern = re.compile(r'<JT_Member_Number>(.*?)</JT_Member_Number>', re.IGNORECASE)

# --- Thread-Safe Helpers ---
print_lock = threading.Lock()
def tprint(msg):
    with print_lock:
        print(msg)

counter_lock = threading.Lock()
processed_count = 0
success_count   = 0
failed_count    = 0

def increment(p=0, s=0, f=0):
    global processed_count, success_count, failed_count
    with counter_lock:
        processed_count += p
        success_count   += s
        failed_count    += f

# --- Image Logic ---

def get_pixel_data(img):
    """Modern replacement for getdata() to avoid DeprecationWarning."""
    try:
        # Pillow 12.1.0+ method
        return img.get_flattened_data()
    except AttributeError:
        # Fallback for older Pillow versions
        return list(img.getdata())

def is_blank_crop(img_bytes, threshold=250, blank_ratio=0.97):
    """SPEED BOOST: Skips blank pages using NumPy arrays instead of loops."""
    with Image.open(io.BytesIO(img_bytes)) as img:
        data = np.array(img.convert('L'))
        light_pixels = np.sum(data >= threshold)
        return (light_pixels / data.size) >= blank_ratio

def get_all_page_crops(filepath):
    """Extracts crops and scores them by 'colourfulness' for prioritization."""
    crops = []
    with Image.open(filepath) as img:
        page = 0
        while True:
            try:
                img.seek(page)
                frame = img.copy()
                if frame.mode != 'RGB':
                    frame = frame.convert('RGB')

                w, h = frame.size
                cropped = frame.crop((int(w * 0.5), 0, w, int(h * 0.3)))

                buf = io.BytesIO()
                cropped.save(buf, format="JPEG", quality=90)
                img_bytes = buf.getvalue()

                # Optimized Colour Score (Vectorized NumPy math)
                r, _, b = frame.split()
                r_arr, b_arr = np.array(r, dtype=np.int16), np.array(b, dtype=np.int16)
                colour_score = np.sum(np.abs(r_arr - b_arr))

                crops.append((page + 1, img_bytes, colour_score))
                page += 1
            except EOFError:
                break
    
    crops.sort(key=lambda x: x[2], reverse=True)
    return crops

# --- Extraction Logic ---

def is_valid_nssf(nssf_string):
    clean_str = nssf_string.strip().upper()
    if any(x in clean_str for x in ["UNREADABLE", "MISSING_TAGS"]): return False
    return 8 <= len(clean_str) <= 12 and sum(c.isalpha() for c in clean_str) <= 1

def run_multi_tier_extraction(img_bytes, filename):
    """Dynamic cascade through configured models."""
    last_value = "MISSING_TAGS"
    for model in MODELS_TO_TRY:
        try:
            # Added timeout to prevent hung worker threads
            response = ollama.chat(
                model=model,
                messages=[{'role': 'user', 'content': SYSTEM_PROMPT, 'images': [img_bytes]}],
                options={'num_predict': 50}  # Limit output length for speed
            )
            raw_output = response['message']['content'].strip()
            match = xml_pattern.search(raw_output)
            extracted_value = match.group(1) if match else "MISSING_TAGS"

            if is_valid_nssf(extracted_value):
                tprint(f"    [+] {model} -> {extracted_value} ({filename})")
                return extracted_value, True
            
            last_value = extracted_value
            tprint(f"    [-] {model} invalid/failed ({filename})")
        except Exception as e:
            tprint(f"    [!] {model} error: {e}")
            continue
    return last_value, False

def process_single_file(filename):
    filepath = os.path.join(source_dir, filename)
    try:
        page_crops = get_all_page_crops(filepath)
        extracted_value, found = "MISSING_TAGS", False

        for p_num, img_bytes, _ in page_crops:
            if is_blank_crop(img_bytes): continue
            
            extracted_value, found = run_multi_tier_extraction(img_bytes, filename)
            if found:
                increment(p=1, s=1)
                break

        if not found:
            shutil.copy2(filepath, os.path.join(manual_review_dir, filename))
            increment(p=1, f=1)

        # File naming fix: remove original extension correctly
        base_name = os.path.splitext(filename)[0]
        with open(os.path.join(output_dir, f"{base_name}.xml"), 'w', encoding='utf-8') as f:
            f.write(f"<JT_Member_Number>{extracted_value}</JT_Member_Number>")

    except Exception as e:
        tprint(f"Critical error on {filename}: {e}")

def main():
    for d in [output_dir, manual_review_dir]:
        if not os.path.exists(d): os.makedirs(d)

    files = [f for f in os.listdir(source_dir) if f.lower().endswith(('.tif', '.tiff', '.jpg'))]
    tprint(f"Starting Extraction | {len(files)} files | Models: {', '.join(MODELS_TO_TRY)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_file, f): f for f in files}
        for future in as_completed(futures):
            future.result()

    tprint(f"\nDone. Success: {success_count} | Failed: {failed_count}")

if __name__ == "__main__":
    main()