import os
import io
import re
import shutil
from PIL import Image
import ollama

# --- Configuration ---
source_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\TIFF\_invalid_member_numbers"
output_dir = r'\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\Extracted_XMLs' # Change to where to save XML file
manual_review_dir = os.path.join(output_dir, "Manual_Review_Needed")

# The 3-Tier AI Extraction Team
PRIMARY_MODEL = 'qwen2.5vl'
SECONDARY_MODEL = 'llama3.2-vision'
TERTIARY_MODEL = 'deepseek-ocr'

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

def is_valid_nssf(nssf_string):
    """Validates if the string looks like a real NSSF number."""
    clean_str = nssf_string.strip().upper()
    
    if "UNREADABLE" in clean_str or "MISSING_TAGS" in clean_str:
        return False
    
    if len(clean_str) < 8 or len(clean_str) > 12:
        return False
        
    letter_count = sum(c.isalpha() for c in clean_str)
    if letter_count > 1:
        return False
        
    return True

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

def extract_data_from_tiffs():
    for directory in [output_dir, manual_review_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    processed_count = 0
    success_count = 0
    failed_count = 0
    
    print(f"Starting 3-Tier Extraction...\n1. {PRIMARY_MODEL}\n2. {SECONDARY_MODEL}\n3. {TERTIARY_MODEL}\n")

    for filename in os.listdir(source_dir):
        if not filename.lower().endswith(('.tif', '.tiff', '.jpg')):
            continue

        filepath = os.path.join(source_dir, filename)
        print(f"Processing: {filename}...")

        try:
            with Image.open(filepath) as img:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Crop to top-right quadrant
                width, height = img.size
                left = int(width * 0.5) 
                top = 0
                right = width
                bottom = int(height * 0.3)
                
                cropped_img = img.crop((left, top, right, bottom))
                
                buffered = io.BytesIO()
                cropped_img.save(buffered, format="JPEG")
                img_bytes = buffered.getvalue()

            # --- PASS 1: Qwen ---
            raw_output = ask_ollama(PRIMARY_MODEL, img_bytes)
            match = xml_pattern.search(raw_output)
            extracted_value = match.group(1) if match else "MISSING_TAGS"

            if is_valid_nssf(extracted_value):
                print(f"  [+] {PRIMARY_MODEL} succeeded! Extracted: {extracted_value}")
                success_count += 1
            else:
                print(f"  [!] {PRIMARY_MODEL} failed ('{extracted_value}'). Triggering {SECONDARY_MODEL}...")
                
                # --- PASS 2: Llama ---
                raw_output = ask_ollama(SECONDARY_MODEL, img_bytes)
                match = xml_pattern.search(raw_output)
                extracted_value = match.group(1) if match else "MISSING_TAGS"
                
                if is_valid_nssf(extracted_value):
                    print(f"  [+] {SECONDARY_MODEL} saved the day! Extracted: {extracted_value}")
                    success_count += 1
                else:
                    print(f"  [!] {SECONDARY_MODEL} failed ('{extracted_value}'). Triggering {TERTIARY_MODEL}...")
                    
                    # --- PASS 3: DeepSeek ---
                    raw_output = ask_ollama(TERTIARY_MODEL, img_bytes)
                    match = xml_pattern.search(raw_output)
                    extracted_value = match.group(1) if match else "MISSING_TAGS"

                    if is_valid_nssf(extracted_value):
                        print(f"  [+] {TERTIARY_MODEL} pulled it off! Extracted: {extracted_value}")
                        success_count += 1
                    else:
                        print(f"  [X] All models failed ('{extracted_value}'). Routing to manual review.")
                        shutil.copy2(filepath, os.path.join(manual_review_dir, filename))
                        failed_count += 1

            # Save XML output
            base_name = os.path.splitext(filename)[0]
            new_filename = f"{base_name}.xml"
            new_filepath = os.path.join(output_dir, new_filename)

            with open(new_filepath, 'w', encoding='utf-8') as f:
                f.write(f"<JT_Member_Number>{extracted_value}</JT_Member_Number>")
            
            processed_count += 1
            print("-" * 30)

        except Exception as e:
            print(f"Error processing '{filename}': {e}")

    print(f"\n--- Job Complete ---")
    print(f"Total Processed: {processed_count}")
    print(f"Successful Extractions: {success_count}")
    print(f"Failed/Manual Review: {failed_count}")

if __name__ == "__main__":
    extract_data_from_tiffs()