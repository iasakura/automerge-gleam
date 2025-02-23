import gleam/bit_array
import gleam/list
import gleam/result
import parser/error
import recursive

pub type Parser(a) =
  fn(BitArray) -> Result(#(a, BitArray), error.ParseError)

pub fn do(p: Parser(a), f: fn(a) -> Parser(b)) -> Parser(b) {
  fn(data: BitArray) -> Result(#(b, BitArray), error.ParseError) {
    use #(value, rest) <- result.try(p(data))
    f(value)(rest)
  }
}

pub fn ret(value: a) -> Parser(a) {
  fn(data: BitArray) -> Result(#(a, BitArray), error.ParseError) {
    Ok(#(value, data))
  }
}

pub fn error(err: error.ParseError) -> Parser(a) {
  fn(_data: BitArray) -> Result(#(a, BitArray), error.ParseError) { Error(err) }
}

pub fn repeat(p: Parser(a), n: Int) -> Parser(List(a)) {
  let iter =
    recursive.func2(fn(i, acc, rec) -> Parser(List(a)) {
      case i < n {
        False -> ret(list.reverse(acc))
        True -> {
          use value <- do(p)
          rec(i + 1, [value, ..acc])
        }
      }
    })

  iter(0, [])
}

pub fn n_bytes(n: Int) -> Parser(BitArray) {
  fn(data: BitArray) {
    let size = n * 8
    case data {
      <<bytes:size(size)-bits, rest:bits>> -> Ok(#(bytes, rest))
      _ -> Error(error.InvalidEOF)
    }
  }
}

pub fn lit(prefix: BitArray) -> Parser(#()) {
  use data <- do(n_bytes(bit_array.byte_size(prefix)))
  case prefix == data {
    True -> ret(#())
    False -> error(error.InvalidLiteral(bit_array.inspect(prefix)))
  }
}

pub fn is_empty() -> Parser(Bool) {
  fn(data: BitArray) {
    case data {
      <<>> -> Ok(#(True, data))
      _ -> Ok(#(False, data))
    }
  }
}

pub fn get_bit_array() -> Parser(BitArray) {
  fn(data: BitArray) { Ok(#(data, <<>>)) }
}
