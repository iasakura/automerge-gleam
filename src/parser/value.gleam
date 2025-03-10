import gleam/bit_array
import gleam/int
import gleam/result
import parser/error
import parser/parser.{type Parser, do, ret}
import parser/primitives
import parser/var_int

pub type ValueMetadata {
  // len = 0
  NullValueMetadata(len: Int)
  // len = 0
  FalseValueMetadata(len: Int)
  // len = 0
  TrueValueMetadata(len: Int)
  // len = 1..10
  UIntValueMetadata(len: Int)
  // len = 1..10
  IntValueMetadata(len: Int)
  // len = 8
  FloatValueMetadata(len: Int)
  // len = 0..2^60
  UTF8StringValueMetadata(len: Int)
  // len = 0..2^60
  BytesValueMetadata(len: Int)
  // len = 1..10
  CounterValueMetadata(len: Int)
  // len = 1..10
  TimestampValueMetadata(len: Int)
  // unknown case
  UnknownValueMetadata(len: Int)
}

pub fn decode_value_metadata() -> Parser(ValueMetadata) {
  use metadata <- do(var_int.decode_uint())
  let type_ = int.bitwise_and(metadata, 0b1111)
  let len = int.bitwise_shift_right(metadata, 4)
  case type_ {
    0 -> ret(NullValueMetadata(len))
    1 -> ret(FalseValueMetadata(len))
    2 -> ret(TrueValueMetadata(len))
    3 -> ret(UIntValueMetadata(len))
    4 -> ret(IntValueMetadata(len))
    5 -> ret(FloatValueMetadata(len))
    6 -> ret(UTF8StringValueMetadata(len))
    7 -> ret(BytesValueMetadata(len))
    8 -> ret(CounterValueMetadata(len))
    9 -> ret(TimestampValueMetadata(len))
    _ -> ret(UnknownValueMetadata(len))
  }
}

pub fn decode_value(metadata: ValueMetadata) -> Parser(primitives.RawValue) {
  case metadata {
    NullValueMetadata(_) -> ret(primitives.Null)
    FalseValueMetadata(_) -> ret(primitives.Bool(False))
    TrueValueMetadata(_) -> ret(primitives.Bool(True))
    UIntValueMetadata(_) -> {
      use n <- do(var_int.decode_uint())
      ret(primitives.UInt(n))
    }
    IntValueMetadata(_) -> {
      use n <- do(var_int.decode_int())
      ret(primitives.Int(n))
    }
    FloatValueMetadata(_) -> {
      use bin <- do(parser.n_bytes(8))
      case bin {
        <<n:float>> -> ret(primitives.Float(n))
        _ -> parser.ret_error(error.InvalidFloat)
      }
    }
    UTF8StringValueMetadata(len) -> {
      use bytes <- do(parser.n_bytes(len))
      let str =
        bytes
        |> bit_array.to_string
        // TODO: replace invalid utf8 characters with U+FFFD
        |> result.map_error(fn(_) { error.InvalidUTF8 })
      use str <- do(parser.from_result(str))
      ret(primitives.Str(str))
    }
    BytesValueMetadata(len) -> {
      use bytes <- do(parser.n_bytes(len))
      ret(primitives.Bytes(bytes))
    }
    CounterValueMetadata(_) -> {
      use n <- do(var_int.decode_int())
      ret(primitives.Counter(n))
    }
    TimestampValueMetadata(_) -> {
      use n <- do(var_int.decode_int())
      ret(primitives.Timestamp(n))
    }
    UnknownValueMetadata(len) -> {
      use bytes <- do(parser.n_bytes(len))
      ret(primitives.Unknown(bytes))
    }
  }
}
