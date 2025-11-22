import sys

filename = "furia-vs-falcons-m1-inferno-11-16.dem"

# Put the offsets you found here
candidates = [
    23831857,
    30496292,
    64076048,
    259624318,
    373938816, 
    379517661, 
    380336051
]

def clean_text(data):
    """Converts bytes to printable ASCII, replacing others with '.'"""
    return "".join([chr(b) if 32 <= b <= 126 else '.' for b in data])

print(f"Inspecting {filename}...\n")

with open(filename, "rb") as f:
    for offset in candidates:
        # Go to 30 bytes BEFORE the timestamp to see context
        start_pos = max(0, offset - 30)
        f.seek(start_pos)
        
        # Read 100 bytes (enough to see map names or keys)
        chunk = f.read(100)
        
        # Find where our timestamp sits in this chunk
        relative_offset = offset - start_pos
        
        print(f"--- Offset {offset} ---")
        print(f"Hex Context: {chunk.hex()}")
        print(f"Text Context: {clean_text(chunk)}")
        
        # Visual pointer to where the timestamp starts
        pointer = " " * relative_offset + "^^^^ (Timestamp Here)"
        print(f"              {pointer}")
        print("\n")