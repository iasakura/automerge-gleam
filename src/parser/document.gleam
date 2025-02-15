import parser/change
import parser/error

pub type Document {
  Document(changes: List(change.Change), operations: List(change.Operation))
}

pub fn decode_document(
  _data: BitArray,
) -> Result(#(Document, BitArray), error.ParseError) {
  todo
}
