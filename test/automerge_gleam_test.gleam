import gleam/io
import gleam/string
import gleeunit
import gleeunit/should
import parser/chunk
import parser/error
import parser/parser
import parser/var_int
import simplifile

pub fn main() {
  gleeunit.main()
}

// gleeunit test functions end in `_test`
pub fn hello_world_test() {
  1
  |> should.equal(1)
}

pub fn encode_uint_test() {
  var_int.encode_uint(8)
  |> should.equal(<<8:size(8)>>)
}

pub fn encode_uint2_test() {
  var_int.encode_uint(1335)
  |> should.equal(<<183:size(8), 10:size(8)>>)
}

pub fn decode_uint_test() {
  var_int.decode_uint()(<<8:size(8)>>)
  |> should.equal(Ok(#(8, <<>>)))
}

pub fn decode_uint2_test() {
  var_int.decode_uint()(<<183:size(8), 10:size(8)>>)
  |> should.equal(Ok(#(1335, <<>>)))
}

pub fn decode_uint_error_test() {
  var_int.decode_uint()(<<183:size(8)>>)
  |> should.equal(Error(error.InvalidVarInt))
}

pub fn encode_int_test() {
  var_int.encode_int(8)
  |> should.equal(<<8:size(8)>>)
}

pub fn encode_int2_test() {
  var_int.encode_int(-1)
  |> should.equal(<<127:size(8)>>)
}

pub fn encode_int3_test() {
  var_int.encode_int(-1335)
  |> should.equal(<<201:size(8), 117:size(8)>>)
}

pub fn decode_int_test() {
  var_int.decode_int()(<<8:size(8)>>)
  |> should.equal(Ok(#(8, <<>>)))
}

pub fn decode_int2_test() {
  var_int.decode_int()(<<0x7f:size(8)>>)
  |> should.equal(Ok(#(-1, <<>>)))
}

pub fn decode_int3_test() {
  var_int.decode_int()(<<201:size(8), 117:size(8)>>)
  |> should.equal(Ok(#(-1335, <<>>)))
}

pub fn decode_change1_test() {
  let assert Ok(chunk) = simplifile.read_bits("test-gen/change1.bin")
  let assert Ok(res) = chunk.decode_chunk() |> parser.run(chunk)
  io.debug(string.inspect(res))
}

pub fn decode_change2_test() {
  let assert Ok(chunk) = simplifile.read_bits("test-gen/change2.bin")
  let assert Ok(res) = chunk.decode_chunk() |> parser.run(chunk)
  io.debug(string.inspect(res))
}
