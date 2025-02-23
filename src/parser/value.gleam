pub type ValueMetadata {
  // len = 0
  NullValueMetadata(len: Int)
  // len = 0
  FalseValueMetadata(len: Int)
  // len = 0
  TrueValueMetadata(len: Int)
  // len = 1..10
  UIntValueMetadata(len: Int)
  // len = 1..10
  IntValueMetadata(len: Int)
  // len = 8
  FloatValueMetadata(len: Int)
  // len = 0..2^60
  UTF8StringValueMetadata(len: Int)
  // len = 0..2^60
  BytesValueMetadata(len: Int)
  // len = 1..10
  CounterValueMetadata(len: Int)
  // len = 1..10
  TimestampValueMetadata(len: Int)
  // unknown case
  UnknownValueMetadata(len: Int)
}
