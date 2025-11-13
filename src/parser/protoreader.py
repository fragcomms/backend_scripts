class ProtoReader:
    """
    Minimal Protobuf reader for PBDEMS2 headers.
    Supports:
      - varint (wire type 0)
      - length-delimited (wire type 2)
      - 32-bit (wire type 5)
      - 64-bit (wire type 1)
    """

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def eof(self):
        return self.pos >= len(self.data)

    def _read_byte(self):
        if self.pos >= len(self.data):
            raise EOFError("Out of bytes")
        b = self.data[self.pos]
        self.pos += 1
        return b

    def read_varint(self):
        """Standard protobuf varint."""
        result = 0
        shift = 0
        while True:
            b = self._read_byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        return result

    def read_key(self):
        """Return (field_number, wire_type)."""
        key = self.read_varint()
        field_number = key >> 3
        wire_type = key & 0x07
        return field_number, wire_type

    def read_length_delimited(self):
        length = self.read_varint()
        end = self.pos + length
        if end > len(self.data):
            raise EOFError("Field length exceeds buffer")
        out = self.data[self.pos:end]
        self.pos = end
        return out

    def read_fixed32(self):
        out = self.data[self.pos:self.pos+4]
        self.pos += 4
        return int.from_bytes(out, "little", signed=False)

    def read_fixed64(self):
        out = self.data[self.pos:self.pos+8]
        self.pos += 8
        return int.from_bytes(out, "little", signed=False)

    def skip_field(self, wire_type):
        if wire_type == 0:
            self.read_varint()
        elif wire_type == 1:
            self.read_fixed64()
        elif wire_type == 2:
            self.read_length_delimited()
        elif wire_type == 5:
            self.read_fixed32()
        else:
            raise ValueError(f"Unsupported wire type: {wire_type}")
