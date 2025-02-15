import gleam/option
import parser/error
import parser/primitives

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
    deps: List(Change),
    time: option.Option(Int),
    message: option.Option(String),
    extra_data: BitArray,
  )
}

pub fn decode_change(
  _data: BitArray,
) -> Result(#(Change, BitArray), error.ParseError) {
  todo
}

pub fn decode_compressed_change(
  _data: BitArray,
) -> Result(#(Change, BitArray), error.ParseError) {
  todo
}
