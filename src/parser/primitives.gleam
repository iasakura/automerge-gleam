pub type ActorId =
  BitArray

pub type OperationId {
  OperationId(ActorId, Int)
}

pub type ObjectId =
  OperationId

pub fn object_id(actor_id: ActorId, counter: Int) -> ObjectId {
  OperationId(actor_id, counter)
}

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
  Unknown(BitArray)
}

pub type Action {
  MakeMap
  Set
  MakeList
  Del
  MakeText
  Inc
}
