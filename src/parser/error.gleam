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
  InvalidFloat
  MissingValueMetadata
  InvalidColumnLength
  TodoNotImplemented
  InvalidValueMetadata
  MissingObjectActorId
  MissingObjectCounter
  MissingKeyActorId
  MissingKeyCounter
  MissingKeyString
  MissingActorId
  MissingCounter
  MissingInsert
  MissingAction
  MissingValue
  MissingPredecessorGroup
  MissingPredecessorActorId
  MissingPredecessorCounter
  MissingSuccessorGroup
  MissingSuccessorActorId
  MissingSuccessorCounter
  InvalidOperationColumns
  InvalidActorIndex
  InvalidKey
  InvalidId
  InvalidAction
  InvalidPredecessors
  InvalidSuccessors
}
