import parser/change
import parser/error
import parser/parser.{type Parser}

pub type Document {
  Document(changes: List(change.Change), operations: List(change.Operation))
}

pub fn decode_document() -> Parser(Document) {
  parser.ret_error(error.TodoNotImplemented)
}
