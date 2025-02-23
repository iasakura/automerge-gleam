import gleam/bit_array
import gleam/list
import gleam/option
import gleam/result.{try}
import parser/column
import parser/error
import parser/primitives
import parser/util
import parser/var_int
import recursive

pub type ChangeHash =
  BitArray

pub type Operation {
  Operation(
    actor_id: primitives.ObjectId,
    key: primitives.Key,
    id: primitives.OperationId,
    insert: Bool,
    value: option.Option(primitives.RawValue),
    successors: List(Operation),
  )
}

pub type Change {
  Change(
    actor_id: BitArray,
    seq: Int,
    operations: List(Operation),
    deps: List(ChangeHash),
    time: option.Option(Int),
    message: option.Option(String),
    other_actors: List(primitives.ActorId),
    extra_data: BitArray,
  )
}

fn decode_change_hashes_loop(i, acc, rest) {
  todo
}

pub fn decode_change_hashes(
  data: BitArray,
) -> Result(#(List(BitArray), BitArray), error.ParseError) {
  use #(n, rest) <- try(var_int.decode_uint(data))
  let size = 32 * 8
  let iter =
    recursive.func3(fn(i, acc, rest, rec) -> Result(
      #(List(BitArray), BitArray),
      error.ParseError,
    ) {
      case i < n {
        False -> Ok(#(list.reverse(acc), rest))
        True -> {
          case rest {
            <<hash:bits-size(size), rest:bits>> ->
              rec(i + 1, [hash, ..acc], rest)
            _ -> Error(error.InvalidEOF)
          }
        }
      }
    })

  iter(0, [], rest)
}

pub fn decode_message(message: BitArray) -> Result(String, error.ParseError) {
  use message <- try(case bit_array.to_string(message) {
    Ok(message) -> Ok(message)
    Error(_) -> Error(error.InvalidUTF8)
  })
  Ok(message)
}

pub fn decode_actor_array(
  data: BitArray,
) -> Result(#(List(primitives.ActorId), BitArray), error.ParseError) {
  use #(n, rest) <- try(var_int.decode_uint(data))
  let iter =
    recursive.func3(fn(i, acc, rest, rec) -> Result(
      #(List(primitives.ActorId), BitArray),
      error.ParseError,
    ) {
      case i < n {
        False -> Ok(#(list.reverse(acc), rest))
        True -> {
          {
            use #(actor_len, rest) <- try(var_int.decode_uint(rest))
            use #(actor_id, rest) <- try(util.n_bytes(actor_len, rest))
            rec(i + 1, [actor_id, ..acc], rest)
          }
        }
      }
    })

  iter(0, [], rest)
}

fn decode_operations(
  _data: BitArray,
  _column_metadata: List(column.ColumnMetadata),
  // for actor columns
  _actor_id: BitArray,
  _other_actors: List(primitives.ActorId),
) -> Result(#(List(Operation), BitArray), error.ParseError) {
  todo
}

pub fn decode_change(data: BitArray) -> Result(Change, error.ParseError) {
  use #(deps, rest) <- try(decode_change_hashes(data))

  use #(actor_len, rest) <- try(var_int.decode_uint(rest))

  use #(actor_id, rest) <- try(util.n_bytes(actor_len, rest))

  use #(seq_num, rest) <- try(var_int.decode_uint(rest))

  use #(time, rest) <- try(var_int.decode_int(rest))

  use #(message_len, rest) <- try(var_int.decode_uint(rest))

  use #(message, rest) <- try(util.n_bytes(message_len, rest))

  use message <- try(decode_message(message))

  use #(other_actors, rest) <- try(decode_actor_array(rest))

  use #(column_metadata, rest) <- try(column.decode_column_metadata(rest))

  use #(ops, rest) <- try(decode_operations(
    rest,
    column_metadata,
    actor_id,
    other_actors,
  ))

  Ok(Change(
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

pub fn decode_compressed_change(
  _data: BitArray,
) -> Result(#(Change, BitArray), error.ParseError) {
  todo
}
