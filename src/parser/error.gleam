pub type ParseError {
  InvalidVarInt
  InvalidMagicNumber
  InvalidEOF
  InvalidCheckSum
  InvalidChunkType
  InternalError
  InvalidDocument
  InvalidChange
  InvalidChunkLength
  InvalidUTF8
  InvalidLiteral(String)
  InvalidDeltaColumn
  MissingValueMetadata
  InvalidColumnLength
  NotImplemented
  InvalidValueMetadata
}
