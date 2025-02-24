import parser/error
import gleam/int
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

pub fn parse_value_metadata() -> Parser(ValueMetadata) {
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

pub fn parse_value(metadata: ValueMetadata) -> Parser(primitives.RawValue) {
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
      // TODO: implement to float parser
      parser.ret_error(error.NotImplemented)
    }
    UTF8StringValueMetadata(len) -> do(parser.n_bytes(len))
    BytesValueMetadata(len) -> do(parser.n_bytes(len))
    CounterValueMetadata(_) -> do(var_int.decode_uint())
    TimestampValueMetadata(_) -> do(var_int.decode_uint())
    UnknownValueMetadata(_) -> ret(primitives.Unknown)
  }
}
