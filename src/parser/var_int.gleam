import gleam/int
import parser/error
import parser/parser

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

fn decode_uint_impl(
  bits: BitArray,
  acc: Int,
  cnt: Int,
) -> Result(#(Int, BitArray), error.ParseError) {
  case bits {
    <<0:size(1), n:size(7), rest:bits>> -> {
      let res = int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7))
      Ok(#(res, rest))
    }
    <<1:size(1), n:size(7), rest:bits>> -> {
      decode_uint_impl(
        rest,
        int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7)),
        cnt + 1,
      )
    }
    _ -> Error(error.InvalidVarInt)
  }
}

fn log2(n: Int) -> Int {
  case n > 1 {
    True -> 1 + log2(int.bitwise_shift_right(n, 1))
    False -> 0
  }
}

fn ceil_log2(n: Int) -> Int {
  case int.bitwise_and(n, n - 1) {
    0 -> log2(n)
    _ -> log2(n - 1) + 1
  }
}

fn n_bits(n: Int) -> Int {
  case n < 0 {
    True -> ceil_log2(-n) + 1
    False -> ceil_log2(n + 1) + 1
  }
}

pub fn decode_uint() -> parser.Parser(Int) {
  let parser = fn(bits: BitArray) -> Result(#(Int, BitArray), error.ParseError) {
    decode_uint_impl(bits, 0, 0)
  }
  parser
}

pub fn encode_int(n: Int) -> BitArray {
  // -2^(m - 1) <= n < 2^(m - 1) -> m >= ceil(log2(n + 1)) + 1 or m >= ceil(log2(-n))
  let nb = n_bits(n)
  let nb = { nb + 6 } / 7 * 7
  let bits = <<n:big-size({ nb / 7 })-unit(7)>>
  fill_ctrl_bits(bits, <<>>)
}

fn fill_ctrl_bits(bits: BitArray, acc: BitArray) -> BitArray {
  case bits, acc {
    <<n:size(7), rest:bits>>, <<>> -> {
      fill_ctrl_bits(rest, <<0:size(1), n:size(7)>>)
    }
    <<n:size(7), rest:bits>>, acc -> {
      fill_ctrl_bits(rest, <<1:size(1), n:size(7), acc:bits>>)
    }
    <<>>, acc -> acc
    _, _ -> panic as "unreachable"
  }
}

fn decode_int_impl(
  bits: BitArray,
  acc: Int,
  cnt: Int,
) -> Result(#(Int, BitArray), error.ParseError) {
  case bits {
    <<0:size(1), n:size(7), rest:bits>> -> {
      let res = int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7))
      let sign = int.bitwise_and(n, 0x40)
      let res = case sign > 0 {
        True -> res - int.bitwise_shift_left(1, { cnt + 1 } * 7)
        False -> res
      }
      Ok(#(res, rest))
    }
    <<1:size(1), n:size(7), rest:bits>> -> {
      decode_int_impl(
        rest,
        int.bitwise_or(acc, int.bitwise_shift_left(n, cnt * 7)),
        cnt + 1,
      )
    }
    _ -> Error(error.InvalidVarInt)
  }
}

pub fn decode_int() -> parser.Parser(Int) {
  let parser = fn(bits: BitArray) -> Result(#(Int, BitArray), error.ParseError) {
    decode_int_impl(bits, 0, 0)
  }
  parser
}
