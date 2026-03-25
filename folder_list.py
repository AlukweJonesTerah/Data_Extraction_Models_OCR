import os
from datetime import datetime

def list_2026_folders(network_path):
    folders = []
    skipped_count = 0
    
    try:
        items = os.listdir(network_path)
        
        for item in items:
            full_path = os.path.join(network_path, item)
            
            try:
                if os.path.isdir(full_path):
                    # Check both Creation (ctime) and Modification (mtime)
                    ctime = os.path.getctime(full_path)
                    mtime = os.path.getmtime(full_path)
                    
                    c_date = datetime.fromtimestamp(ctime)
                    m_date = datetime.fromtimestamp(mtime)
                    
                    # If EITHER date is in 2026, we count it
                    if c_date.year == 2026 or m_date.year == 2026:
                        # We use the creation date for the display
                        folders.append({
                            'name': item,
                            'created': c_date.strftime('%Y-%m-%d %H:%M:%S'),
                            'raw_time': ctime
                        })
            except OSError:
                # This happens if a folder is locked or permissions are denied
                skipped_count += 1
                continue
        
        # Sort by creation time
        folders.sort(key=lambda x: x['raw_time'])

        for folder in folders:
            print(f"📁 {folder['name']}  |  Created: {folder['created']}")
        
        print(f"\n--- Summary ---")
        print(f"Total 2026 folders found: {len(folders)}")
        if skipped_count > 0:
            print(f"Folders skipped due to access errors: {skipped_count}")

        return folders

    except Exception as e:
        print(f"Critical Error: {e}")
        return []

path = r"\\192.168.1.11\phase_v\BCERTS\RAW"
list_2026_folders(path)