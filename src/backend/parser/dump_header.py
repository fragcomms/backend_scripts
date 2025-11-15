from protoreader import ProtoReader

DEMO_FILE = "the-mongolz-vs-astralis-m1-nuke.dem"

def main():
    with open(DEMO_FILE, "rb") as f:
        data = f.read()

    magic = data[:8]
    print("Magic:", magic, magic.decode(errors="replace"))
    print("=" * 50)

    marker = b"\x0a\x08PBDEMS2\x00"
    start = data.find(marker)
    if start == -1:
        raise RuntimeError("Could not find protobuf header start")

    header_bytes = data[start:]

    pr = ProtoReader(header_bytes)

    seen_first_field1 = False

    while not pr.eof():
        try:
            field_no, wire_type = pr.read_key()
        except EOFError:
            break

        # if we see field 1 a second time, that's the next message → stop
        if field_no == 1 and seen_first_field1:
            print("\n-- Reached next message; header parsing complete --")
            break
        if field_no == 1:
            seen_first_field1 = True

        print(f"Field {field_no}, wire {wire_type}: ", end="")

        if wire_type == 0:
            val = pr.read_varint()
            print(f"varint {val}")
        elif wire_type == 1:
            val = pr.read_fixed64()
            print(f"fixed64 {val}")
        elif wire_type == 2:
            raw = pr.read_length_delimited()
            try:
                s = raw.decode("utf-8")
                print(f'string "{s}"')
            except UnicodeDecodeError:
                print(f"bytes {raw.hex()} (len={len(raw)})")
        elif wire_type == 5:
            val = pr.read_fixed32()
            print(f"fixed32 {val}")
        else:
            print(f"[unknown wire type {wire_type}], stopping")
            break


if __name__ == "__main__":
    main()
