pub type ActorId =
  BitArray

pub type OperationId =
  #(ActorId, Int)

pub type ObjectId =
  OperationId

pub type Key {
  StringKey(key: String)
  OperationKey(key: OperationId)
}

pub type RawValue {
  Null
  Bool(Bool)
  UInt(Int)
  Int(Int)
  Float(Float)
  Str(String)
  Bytes(BitArray)
  Counter(Int)
  Timestamp(Int)
}
