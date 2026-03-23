import json

def clean_nssf_dataset_universal(json_path, output_path):
    print(f"Reading from: {json_path}")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cleaned_data = []
    processed_count = 0
    
    for record in data:
        try:
            messages = record.get('messages', [])
            if len(messages) < 2: continue
            
            # Extract the JSON string from the assistant's response
            assistant_content = messages[1].get('content', '')
            entities = json.loads(assistant_content)
            
            # --- 1. Fix the 'Instution' typo ---
            if 'Instution' in entities:
                entities['Institution'] = entities.pop('Instution')
            
            # --- 2. Standardize Institution Name ---
            if 'Institution' in entities:
                entities['Institution'] = "NATIONAL SOCIAL SECURITY FUND"
            
            # --- 3. Broad Normalization (No Skipping) ---
            raw_doc_type = str(entities.get('Document_Type', '')).upper()
            
            if 'MEMBERSHIP' in raw_doc_type:
                entities['Document_Type'] = 'MEMBERSHIP CARD'
            elif 'REGISTRATION' in raw_doc_type:
                entities['Document_Type'] = 'REGISTRATION CARD'
            
            # --- 4. Re-pack and Save ---
            messages[1]['content'] = json.dumps(entities)
            record['messages'] = messages
            cleaned_data.append(record)
            processed_count += 1
            
        except Exception as e:
            continue
    
    print(f"Saving to: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, indent=2)
    
    print(f"Done! Processed {processed_count} out of {len(data)} records.")

# CRITICAL FIX: Explicit file paths
input_file = r"C:\Users\IT\Documents\ultimate_training_dataset.json"
output_file = r"C:\Users\IT\Desktop\MVP_Extraction\cleaned_training_dataset.json"

clean_nssf_dataset_universal(input_file, output_file)