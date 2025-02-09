import gleam/float
import gleam/int

fn encode_uint_impl(n: Int, acc: BitArray) -> BitArray {
  case n < 0x80 {
    True -> <<acc:bits, n:size(8)>>
    False -> {
      let next_chunk = int.bitwise_or(int.bitwise_and(n, 0x7f), 0x80)
      encode_uint_impl(int.bitwise_shift_right(n, 7), <<
        acc:bits,
        next_chunk:size(8),
      >>)
    }
  }
}

pub fn encode_uint(n) -> BitArray {
  encode_uint_impl(n, <<>>)
}

pub type ParseError {
  InvalidVarInt
}

fn decode_uint_impl(
  bits: BitArray,
  acc: Int,
  cnt: Int,
) -> Result(#(Int, BitArray), ParseError) {
  case bits {
    <<n:size(8), rest:bits>> if n < 0x80 -> {
      let res = int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7))
      Ok(#(res, rest))
    }
    <<n:size(8), rest:bits>> -> {
      let n = int.bitwise_and(n, 0x7f)
      decode_uint_impl(
        rest,
        int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7)),
        cnt + 1,
      )
    }
    _ -> Error(InvalidVarInt)
  }
}

@external(erlang, "math", "log2")
fn log2(f: Float) -> Float

pub fn decode_uint(bits) -> Result(#(Int, BitArray), ParseError) {
  decode_uint_impl(bits, 0, 0)
}

pub fn encode_int(n: Int) -> BitArray {
  let n_bits = case n < 0 {
    True -> float.truncate(float.ceiling(log2(int.to_float(-n)))) + 1
    False -> float.truncate(float.ceiling(log2(int.to_float(n) +. 1.0)))
  }
  let n_bits = { n_bits + 6 } / 7 * 7
  let bits = <<n:big-size({ n_bits / 7 })-unit(7)>>
  fill_bits(bits, <<>>)
}

fn fill_bits(bits: BitArray, acc: BitArray) -> BitArray {
  case bits, acc {
    <<n:size(7), rest:bits>>, <<>> -> {
      fill_bits(rest, <<0:size(1), n:size(7)>>)
    }
    <<n:size(7), rest:bits>>, acc -> {
      fill_bits(rest, <<1:size(1), n:size(7), acc:bits>>)
    }
    <<>>, acc -> acc
    _, _ -> panic as "unreachable"
  }
}
