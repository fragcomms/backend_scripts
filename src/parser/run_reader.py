from bitreader import BitReader

# CHANGE THIS to your actual demo file name
DEMO_FILE = "the-mongolz-vs-astralis-m1-nuke.dem"

def main():
    with open(DEMO_FILE, "rb") as f:
        data = f.read()

    # Create the bit reader
    br = BitReader(data)

    # Example: read the first 8 bytes as raw bytes
    first8 = br.read_bytes(8)
    print("Magic bytes:", first8, first8.decode(errors="replace"))

    # Example: read next 16 bits
    next16 = br.read_bits(16)
    print("Next 16 bits:", next16)

    # Example: read a boolean
    b = br.read_bool()
    print("Next bit as bool:", b)

    print("Current bit position:", br.bitpos)

if __name__ == "__main__":
    main()