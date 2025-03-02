import gleam/bit_array
import gleam/list
import gleam/option
import gleam/result.{try}
import parser/column
import parser/error
import parser/operation
import parser/parser.{type Parser, do, ret, ret_error}
import parser/primitives
import parser/var_int
import recursive

pub type ChangeHash =
  BitArray

pub type Change {
  Change(
    actor_id: BitArray,
    seq: Int,
    operations: List(operation.Operation),
    deps: List(ChangeHash),
    time: option.Option(Int),
    message: option.Option(String),
    other_actors: List(primitives.ActorId),
    extra_data: BitArray,
  )
}

pub fn decode_change_hashes() -> Parser(List(ChangeHash)) {
  use n <- do(var_int.decode_uint())
  let size = 32
  let iter =
    recursive.func2(fn(i, acc, rec) {
      case i < n {
        False -> ret(list.reverse(acc))
        True -> {
          use hash <- do(parser.n_bytes(size))
          rec(i + 1, [hash, ..acc])
        }
      }
    })
  iter(0, [])
}

pub fn decode_message(message: BitArray) -> Result(String, error.ParseError) {
  use message <- try(case bit_array.to_string(message) {
    Ok(message) -> Ok(message)
    Error(_) -> Error(error.InvalidUTF8)
  })
  Ok(message)
}

pub fn decode_actor_array() -> Parser(List(primitives.ActorId)) {
  use n <- do(var_int.decode_uint())
  let iter =
    recursive.func2(fn(i, acc, rec) {
      case i < n {
        False -> ret(list.reverse(acc))
        True -> {
          {
            use actor_len <- do(var_int.decode_uint())
            use actor_id <- do(parser.n_bytes(actor_len))
            rec(i + 1, [actor_id, ..acc])
          }
        }
      }
    })

  iter(0, [])
}

pub fn decode_change() -> Parser(Change) {
  use deps <- do(decode_change_hashes())

  use actor_len <- do(var_int.decode_uint())

  use actor_id <- do(parser.n_bytes(actor_len))

  use seq_num <- do(var_int.decode_uint())

  use time <- do(var_int.decode_int())

  use message_len <- do(var_int.decode_uint())

  use message <- do(parser.n_bytes(message_len))

  use message <- do(case decode_message(message) {
    Ok(message) -> ret(message)
    Error(_) -> ret_error(error.InvalidUTF8)
  })

  use other_actors <- do(decode_actor_array())

  use column_metadata <- do(column.decode_column_metadata())

  use ops <- do(operation.decode_operations(
    column_metadata,
    actor_id,
    other_actors,
  ))

  use rest <- do(parser.take_rest())

  ret(Change(
    actor_id,
    seq_num,
    operations: ops,
    deps: deps,
    time: option.Some(time),
    message: option.Some(message),
    other_actors: other_actors,
    extra_data: rest,
  ))
}

pub fn decode_compressed_change() -> Parser(Change) {
  parser.ret_error(error.TodoNotImplemented)
}
