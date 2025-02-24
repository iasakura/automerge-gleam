import gleam/result.{try}
import parser/error

pub fn n_bytes(
  n: Int,
  data: BitArray,
) -> Result(#(BitArray, BitArray), error.ParseError) {
  let size = n * 8
  case data {
    <<bytes:size(size)-bits, rest:bits>> -> Ok(#(bytes, rest))
    _ -> Error(error.InvalidEOF)
  }
}
