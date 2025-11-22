import mmap
import datetime

filename = "furia-vs-falcons-m1-inferno-11-16.dem"

# Strict Range: Narrow this as much as possible to reduce noise
MIN_TS = 1763272800  # Nov 15, 2025
MAX_TS = 1763300580  # Nov 17, 2025
#1763280000000

def read_varint_generator(mm):
    """
    Yields (offset, value) for every varint in the file.
    """
    i = 0
    length = len(mm)
    
    while i < length:
        # Check if we can read a varint here
        result = 0
        shift = 0
        valid = True
        
        # Try to read up to 5 bytes
        for byte_offset in range(5):
            if i + byte_offset >= length:
                valid = False
                break
                
            b = mm[i + byte_offset]
            result |= (b & 0x7F) << shift
            
            if not (b & 0x80):
                # End of VarInt
                yield i, result
                break
            
            shift += 7
            if shift >= 35: # VarInt too big to be a timestamp
                valid = False
                break
        
        # Move forward 1 byte to brute force every position
        i += 1

print(f"Brute force scanning {filename}...")

with open(filename, "rb") as f:
    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        count = 0
        for offset, val in read_varint_generator(mm):
            if MIN_TS < val < MAX_TS:
                dt = datetime.datetime.fromtimestamp(val)
                print(f"Offset {offset}: {val} -> {dt}")
                count += 1
                
                # Safety break if we find too many (it means our filter is too loose)