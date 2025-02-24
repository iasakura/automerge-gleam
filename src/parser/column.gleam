import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result.{try}
import parser/error
import parser/parser.{type Parser, do, ret, ret_error}
import parser/primitives
import parser/value
import parser/var_int
import recursive

pub type Column {
  // RLE uLEB
  GroupColumn(List(Option(Int)))
  // Actor index, rle
  ActorColumn(List(Option(Int)))
  // RLE
  ULEBColumn(List(Option(Int)))
  // diff RLE
  DeltaColumn(List(Int))
  // #false -> #true -> #false -> ...
  BooleanColumn(List(Bool))
  // RLE of length-prefixed string
  StringColumn(List(Option(String)))
  // RLE of metadata
  ValueMetadataColumn(List(value.ValueMetadata))
  // specified in metadata
  ValueColumn(List(primitives.RawValue))
  UnknownColumn(BitArray)
}

pub type ColumnType {
  Group
  Actor
  ULEB
  Delta
  Boolean
  String
  ValueMetadata
  Value
  Unknown
}

pub type ColumnSpec {
  ColumnSpec(id: Int, column_type: ColumnType, deflate: Bool)
}

pub type ColumnMetadata {
  ColumnMetadata(column_spec: ColumnSpec, column_len: Int)
}

pub fn decode_column_spec() -> Parser(ColumnSpec) {
  use metadata <- do(var_int.decode_uint())
  let type_ = int.bitwise_and(metadata, 0b111)
  let type_ = case type_ {
    0 -> Group
    1 -> Actor
    2 -> ULEB
    3 -> Delta
    4 -> Boolean
    5 -> String
    6 -> ValueMetadata
    7 -> Value
    _ -> Unknown
  }
  let deflated = int.bitwise_and(metadata, 0b1000) != 0
  let id = int.bitwise_shift_right(metadata, 4)

  ret(ColumnSpec(id, type_, deflated))
}

pub fn decode_column_metadata() -> Parser(List(ColumnMetadata)) {
  use n <- do(var_int.decode_uint())
  let iter =
    recursive.func2(fn(i, acc, rec) -> Parser(List(ColumnMetadata)) {
      case i < n {
        False -> ret(list.reverse(acc))
        True -> {
          {
            use column_spec <- do(decode_column_spec())
            use column_len <- do(var_int.decode_uint())
            rec(i + 1, [ColumnMetadata(column_spec, column_len), ..acc])
          }
        }
      }
    })

  iter(0, [])
}

fn parse_rle(parser: Parser(a)) -> Parser(List(Option(a))) {
  let iter =
    recursive.func(fn(acc, rec) -> Parser(List(Option(a))) {
      use empty <- do(parser.is_empty())
      case empty {
        True -> ret(list.reverse(acc))
        False -> {
          use len <- do(var_int.decode_int())
          case len {
            _ if len < 0 -> {
              let len = -len
              use value <- do(parser.repeat(parser, len))
              rec(list.append(list.map(value, Some), acc))
            }
            _ if len == 0 -> {
              use value <- do(var_int.decode_uint())
              rec(list.append(list.repeat(None, value), acc))
            }
            _ -> {
              use value <- do(parser)
              rec(list.append(list.repeat(Some(value), len), acc))
            }
          }
        }
      }
    })

  iter([])
}

fn parse_delta_column() -> Parser(List(Int)) {
  use rle <- do(parse_rle(var_int.decode_int()))
  let res =
    list.fold(rle, Ok(#([], 0)), fn(acc, value) {
      use acc <- try(acc)
      case value {
        Some(value) -> {
          let #(res, acc) = acc
          let next = acc + value
          Ok(#([next, ..res], next))
        }
        None -> Error(error.InvalidDeltaColumn)
      }
    })
  case res {
    Ok(#(res, _)) -> ret(list.reverse(res))
    Error(err) -> ret_error(err)
  }
}

fn parse_boolean_column() -> Parser(List(Bool)) {
  let iter =
    recursive.func(fn(acc, rec) {
      let #(cur, acc) = acc
      use empty <- do(parser.is_empty())
      case empty {
        True -> ret(list.reverse(acc))
        False -> {
          use len <- do(var_int.decode_uint())
          rec(#(!cur, list.append(list.repeat(cur, len), acc)))
        }
      }
    })
  iter(#(False, []))
}

fn parse_string() -> Parser(String) {
  use len <- do(var_int.decode_uint())
  use str <- do(parser.n_bytes(len))
  case bit_array.to_string(str) {
    Ok(str) -> ret(str)
    Error(_) -> ret_error(error.InvalidUTF8)
  }
}

fn parse_string_column() -> Parser(List(Option(String))) {
  parse_rle(parse_string())
}

fn parse_value_metadata_column() -> Parser(List(value.ValueMetadata)) {
  parser.ret_error(error.NotImplemented)
}

fn parse_value_column(
  _value_metadata: List(value.ValueMetadata),
) -> Parser(List(primitives.RawValue)) {
  parser.ret_error(error.NotImplemented)
}

fn decode_column(
  metadata: ColumnMetadata,
  value_metadata_map: dict.Dict(Int, List(value.ValueMetadata)),
) -> Parser(Column) {
  use data <- do(parser.n_bytes(metadata.column_len))
  let parser = case metadata.column_spec.column_type {
    Group -> {
      use res <- do(parse_rle(var_int.decode_uint()))
      ret(GroupColumn(res))
    }
    Actor -> {
      use res <- do(parse_rle(var_int.decode_uint()))
      ret(ActorColumn(res))
    }
    ULEB -> {
      use res <- do(parse_rle(var_int.decode_uint()))
      ret(ULEBColumn(res))
    }
    Delta -> {
      use res <- do(parse_delta_column())
      ret(DeltaColumn(res))
    }
    Boolean -> {
      use res <- do(parse_boolean_column())
      ret(BooleanColumn(res))
    }
    String -> {
      use res <- do(parse_string_column())
      ret(StringColumn(res))
    }
    ValueMetadata -> {
      use res <- do(parse_value_metadata_column())
      ret(ValueMetadataColumn(res))
    }
    Value -> {
      let id = metadata.column_spec.id
      use value_metadata <- do(case dict.get(value_metadata_map, id) {
        Ok(value_metadata) -> ret(value_metadata)
        Error(_) -> ret_error(error.MissingValueMetadata)
      })
      use res <- do(parse_value_column(value_metadata))
      ret(ValueColumn(res))
    }
    Unknown -> {
      use res <- do(parser.get_bit_array())
      ret(UnknownColumn(res))
    }
  }
  case parser(data) {
    Ok(#(column, <<>>)) -> ret(column)
    Ok(#(_, _)) -> ret_error(error.InvalidColumnLength)
    Error(err) -> ret_error(err)
  }
}

pub fn decode_columns_impl(
  column_metadata: List(ColumnMetadata),
  value_metadata_map: dict.Dict(Int, List(value.ValueMetadata)),
  acc: List(Column),
) -> Parser(List(Column)) {
  case column_metadata {
    [] -> ret(list.reverse(acc))
    [meta, ..rest] -> {
      use column <- do(decode_column(meta, value_metadata_map))
      let value_metadata_map = case column {
        ValueMetadataColumn(value_metadata) -> {
          dict.insert(value_metadata_map, meta.column_spec.id, value_metadata)
        }
        _ -> value_metadata_map
      }
      decode_columns_impl(rest, value_metadata_map, [column, ..acc])
    }
  }
}

pub fn decode_columns(
  column_metadata: List(ColumnMetadata),
) -> Parser(List(Column)) {
  decode_columns_impl(column_metadata, dict.new(), [])
}
