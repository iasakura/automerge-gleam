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
}
