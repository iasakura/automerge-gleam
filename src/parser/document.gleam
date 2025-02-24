import parser/change
import parser/parser.{type Parser}

pub type Document {
  Document(changes: List(change.Change), operations: List(change.Operation))
}

pub fn decode_document() -> Parser(Document) {
  todo
}
