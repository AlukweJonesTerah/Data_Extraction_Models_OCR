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
output_dir = r'\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Extracted_XMLs_v2'
manual_review_dir = os.path.join(output_dir, "Manual_Review_Needed")

# --- Speed Configuration ---
MAX_WORKERS = 3

# The 3-Tier AI Extraction Team
PRIMARY_MODEL   = 'qwen2.5vl'
SECONDARY_MODEL = 'llama3.2-vision'
TERTIARY_MODEL  = 'deepseek-ocr'

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

# Thread-safe print lock
print_lock = threading.Lock()
def tprint(msg):
    with print_lock:
        print(msg)

# --- Counters ---
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

def is_valid_nssf(nssf_string):
    clean_str = nssf_string.strip().upper()
    if "UNREADABLE" in clean_str or "MISSING_TAGS" in clean_str:
        return False
    if len(clean_str) < 8 or len(clean_str) > 12:
        return False
    if sum(c.isalpha() for c in clean_str) > 1:
        return False
    return True

def is_blank_crop(img_bytes, threshold=250, blank_ratio=0.97):
    """Uses NumPy to skip white/blank pages instantly."""
    with Image.open(io.BytesIO(img_bytes)) as img:
        data = np.array(img.convert('L'))
        light_pixels = np.sum(data >= threshold)
        return (light_pixels / data.size) >= blank_ratio

def get_all_page_crops(filepath):
    """
    Opens a multi-page TIFF and returns cropped image bytes per page.
    Sorts pages by colour score so the certificate page is tried first.
    """
    crops = []
    with Image.open(filepath) as img:
        page = 0
        while True:
            try:
                img.seek(page)
                frame = img.copy()
                if frame.mode != 'RGB':
                    frame = frame.convert('RGB')

                width, height = frame.size
                cropped = frame.crop((int(width * 0.5), 0, width, int(height * 0.3)))

                buffered = io.BytesIO()
                cropped.save(buffered, format="JPEG", quality=90)
                img_bytes = buffered.getvalue()

                # Colour score: certificate has green/yellow header → high R vs B difference
                r, _, b = frame.split()
                r_arr = np.array(r, dtype=np.int16)
                b_arr = np.array(b, dtype=np.int16)
                colour_score = np.sum(np.abs(r_arr - b_arr))

                crops.append((page + 1, img_bytes, colour_score))
                page += 1
            except EOFError:
                break

    # Sort: highest colour score first
    crops.sort(key=lambda x: x[2], reverse=True)
    return crops

def ask_ollama(model_name, image_bytes):
    response = ollama.chat(
        model=model_name,
        messages=[{
            'role': 'user',
            'content': SYSTEM_PROMPT,
            'images': [image_bytes]
        }]
    )
    return response['message']['content'].strip()

def run_3tier_extraction(img_bytes, filename):
    """Runs the 3-tier model cascade. Returns (value, success)."""
    for model in [PRIMARY_MODEL, SECONDARY_MODEL, TERTIARY_MODEL]:
        raw_output = ask_ollama(model, img_bytes)
        match = xml_pattern.search(raw_output)
        extracted_value = match.group(1) if match else "MISSING_TAGS"
        if is_valid_nssf(extracted_value):
            tprint(f"    [+] {model} → {extracted_value}  ({filename})")
            return extracted_value, True
        tprint(f"    [!] {model} failed ('{extracted_value}')  ({filename})")
    return extracted_value, False

def process_single_file(filename):
    """Processes one TIFF file. Runs inside a thread."""
    filepath = os.path.join(source_dir, filename)
    tprint(f"Processing: {filename}...")

    try:
        page_crops = get_all_page_crops(filepath)
        tprint(f"  [i] {len(page_crops)} page(s) | {filename}")

        extracted_value = "MISSING_TAGS"
        found = False

        for page_num, img_bytes, _ in page_crops:
            if is_blank_crop(img_bytes):
                tprint(f"  [~] Page {page_num} is blank, skipping  ({filename})")
                continue

            tprint(f"  [>] Trying page {page_num}  ({filename})")
            extracted_value, found = run_3tier_extraction(img_bytes, filename)
            if found:
                tprint(f"  [✓] Found on page {page_num}: {extracted_value}  ({filename})")
                increment(p=1, s=1)
                break

        if not found:
            tprint(f"  [X] All pages failed → manual review  ({filename})")
            shutil.copy2(filepath, os.path.join(manual_review_dir, filename))
            increment(p=1, f=1)

        base_name = os.path.splitext(filename)[0]
        with open(os.path.join(output_dir, f"{base_name}.xml"), 'w', encoding='utf-8') as f:
            f.write(f"<JT_Member_Number>{extracted_value}</JT_Member_Number>")

        tprint("-" * 30)

    except Exception as e:
        tprint(f"Error processing '{filename}': {e}")

def extract_data_from_tiffs():
    for directory in [output_dir, manual_review_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    all_files = [f for f in os.listdir(source_dir) if f.lower().endswith(('.tif', '.tiff', '.jpg'))]

    print(f"Starting 3-Tier Extraction | {len(all_files)} files | {MAX_WORKERS} workers\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_file, f): f for f in all_files}
        for future in as_completed(futures):
            future.result()

    print(f"\n--- Job Complete ---")
    print(f"Total Processed:        {processed_count}")
    print(f"Successful Extractions: {success_count}")
    print(f"Failed/Manual Review:   {failed_count}")

if __name__ == "__main__":
    extract_data_from_tiffs()