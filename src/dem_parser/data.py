import struct
import datetime

filename = "furia-vs-falcons-m1-inferno-11-16.dem"

# 1. Search for the game directory string "csgo" 
# In Protobuf, this string is usually preceded by its length: \x04csgo
ANCHOR = b"csgo" 

print(f"Searching {filename} for metadata anchor...")

with open(filename, "rb") as f:
    # Jump to the end of the file (The metadata is always in the last 200KB)
    f.seek(0, 2)
    file_size = f.tell()
    search_window = 200 * 1024 # 200KB
    start_pos = max(0, file_size - search_window)
    
    f.seek(start_pos)
    data = f.read()
    
    # Find the anchor
    offset = data.find(ANCHOR)
    
    if offset != -1:
        real_offset = start_pos + offset
        print(f"Anchor 'csgo' found at offset {real_offset}")
        
        # 2. Define the "Kill Zone"
        # The timestamp is usually within 100 bytes BEFORE or AFTER the 'csgo' string
        zone_start = max(0, offset - 100)
        zone_end = min(len(data), offset + 100)
        
        chunk = data[zone_start:zone_end]
        
        print("Scanning the immediate neighborhood for valid timestamps...")
        
        # Helper to read varints from this small chunk
        def read_varint(buf, pos):
            res = 0
            shift = 0
            for k in range(5):
                if pos + k >= len(buf): return None
                b = buf[pos + k]
                res |= (b & 0x7F) << shift
                if not (b & 0x80): return res
                shift += 7
            return None

        # Scan every byte in this small zone
        for i in range(len(chunk)):
            val = read_varint(chunk, i)
            
            # Filter: 2023 to Present (approx 1.67B to 1.74B)
            # 176... is Nov 2025, so we cap it at current time approx 1740000000
            if val and 1672531200 < val < 1740000000:
                dt = datetime.datetime.fromtimestamp(val)
                print("------------------------------------------------")
                print(f"MATCH CONFIRMED!")
                print(f"Offset: {real_offset - 100 + i}")
                print(f"Value:  {val}")
                print(f"Date:   {dt}")
                print(f"Context: It was found {abs(i - 100)} bytes away from 'csgo'")
    else:
        print("Could not find the 'csgo' string in the file footer.")