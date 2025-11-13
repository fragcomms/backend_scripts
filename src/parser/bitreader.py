class BitReader:
    def __init__(self, data: bytes):
        self.data = data
        self.bitpos = 0

    def bits_remaining(self) -> int:
        return len(self.data) * 8 - self.bitpos

    def align_to_byte(self):
        rem = self.bitpos & 7
        if rem:
            self.bitpos += (8 - rem)

    def read_bits(self, n: int) -> int:
        if self.bits_remaining() < n:
            raise EOFError("Out of bits")

        val = 0
        for i in range(n):
            byte_index = self.bitpos >> 3
            bit_index = self.bitpos & 7
            bit = (self.data[byte_index] >> bit_index) & 1
            val |= bit << i
            self.bitpos += 1

        return val

    def read_bool(self):
        return self.read_bits(1) != 0

    def read_bytes(self, n: int) -> bytes:
        self.align_to_byte()
        byte_index = self.bitpos >> 3
        end = byte_index + n
        out = self.data[byte_index:end]
        self.bitpos += n * 8
        return out
