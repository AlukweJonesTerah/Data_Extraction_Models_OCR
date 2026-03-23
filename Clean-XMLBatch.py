import os
import re
import shutil

# Configuration
source_dirs = [
    # r"C:\Users\IT\Documents\omniscan_images"
    # r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML",
    r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\XML\_invalid_member_numbers"
]

# output_dir = r"C:\Users\IT\Documents\cleaned_xml"
output_dir = r"\\192.168.1.11\d\BCERT_Phase4_Omniscan\DMS_Upload\cleaned_xml"

# Regex patterns
tag_pattern = re.compile(r'<JT_Member_Number>(.*?)</JT_Member_Number>')
special_char_pattern = re.compile(r'[^a-zA-Z0-9]')

def clean_xml_files():

    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    except OSError as e:
        print(f"CRITICAL ERROR: Cannot connect to the server or output path! ({e})")
        print("Please check your network connection to \\\\192.168.1.11 and try again.")
        return

    processed_count = 0
    perfect_count = 0

    for folder in source_dirs:
        if not os.path.exists(folder):
            print(f"Folder not found: {folder}")
            continue
        
        for filename in os.listdir(folder):
            if not filename.lower().endswith('.xml'):
                continue

            filepath = os.path.join(folder, filename)

            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    content = f.read()

                match = tag_pattern.search(content)
                if match:
                    original_number = match.group(1)
                    cleaned_number = special_char_pattern.sub('', original_number)

                    if not cleaned_number:
                        cleaned_number = "BLANK_MEMBER"

                    new_filename = f"{cleaned_number}.xml"
                    new_filepath = os.path.join(output_dir, new_filename)

                    # Handle duplicate filenames 
                    counter = 1
                    while os.path.exists(new_filepath):
                        new_filename = f"{cleaned_number}_{counter}.xml"
                        new_filepath = os.path.join(output_dir, new_filename)
                        counter += 1  

                    # Check if the file is already perfect
                    if original_number == cleaned_number and filename.lower() == f"{cleaned_number.lower()}.xml":
                        # File is perfect! Copy it exactly as it is (preserves timestamps too)
                        shutil.copy2(filepath, new_filepath)
                        perfect_count += 1
                        print(f"[PERFECT] Copied exact match: {filename}")
                        continue 

                    # If not perfect, replace the tag and write the new file
                    new_content = content.replace(
                        f'<JT_Member_Number>{original_number}</JT_Member_Number>',
                        f'<JT_Member_Number>{cleaned_number}</JT_Member_Number>'
                    )

                    with open(new_filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)

                    # Copy original timestamps to keep your sorting intact
                    shutil.copystat(filepath, new_filepath)
                    processed_count += 1
                    
                    # DEBUGGING LINE: This tells you exactly what the script is doing
                    print(f"[CLEANED] Original file: {filename} | Number was: '{original_number}' -> Saved as: {new_filename}")

            except Exception as e:
                print(f"Error processing file '{filename}': {e}")

    print(f"\n--- Test Complete ---")
    print(f"Cleaned and Renamed: {processed_count} files.")
    print(f"Already Perfect (just copied over): {perfect_count} files.")

if __name__ == "__main__":
    clean_xml_files()