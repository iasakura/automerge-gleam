import gleam/bit_array
import gleam/crypto
import gleam/result.{try}
import parser/change
import parser/document
import parser/error
import parser/parser.{type Parser, do, ret}
import parser/var_int

pub type Document {
  Document(changes: List(change.Change), operations: List(change.Operation))
}

pub type Chunk {
  DocumentChunk(document: document.Document)
  ChangeChunk(change: change.Change)
  CompressedChangeChunk(change: change.Change)
}

pub fn validate_checksum(content: BitArray, sum: BitArray) -> Bool {
  let actual = crypto.hash(crypto.Sha256, content)
  bit_array.slice(actual, 0, 4) == Ok(sum)
}

pub fn decode_chunk() -> Parser(Chunk) {
  use #() <- do(parser.lit(<<0x85, 0x6f, 0x4a, 0x83>>))

  use sum <- do(parser.n_bytes(4))

  use chunk_type <- do(parser.n_bytes(1))
  use chunk_type <- do(
    parser.from_result(case chunk_type {
      <<0>> | <<1>> | <<2>> -> Ok(chunk_type)
      _ -> Error(error.InvalidChunkType)
    }),
  )

  use chunk_length <- do(var_int.decode_uint())

  use contents <- do(parser.n_bytes(chunk_length))

  let checksum_computed = <<
    chunk_type:bits,
    { var_int.encode_uint(chunk_length) }:bits,
    contents:bits,
  >>

  use #() <- do(
    parser.from_result(case validate_checksum(checksum_computed, sum) {
      True -> Ok(#())
      False -> Error(error.InvalidCheckSum)
    }),
  )

  let parser = case chunk_type {
    <<0>> -> {
      use document <- do(document.decode_document())
      use empty <- do(parser.is_empty())
      case empty {
        True -> ret(DocumentChunk(document))
        False -> parser.ret_error(error.InvalidChunkLength)
      }
    }
    <<1>> -> {
      use change <- do(change.decode_change())
      use empty <- do(parser.is_empty())
      case empty {
        True -> ret(ChangeChunk(change))
        False -> parser.ret_error(error.InvalidChunkLength)
      }
    }
    <<2>> -> {
      use change <- do(change.decode_compressed_change())
      use empty <- do(parser.is_empty())
      case empty {
        True -> ret(CompressedChangeChunk(change))
        False -> parser.ret_error(error.InvalidChunkLength)
      }
    }
    _ -> parser.ret_error(error.InternalError)
  }
  parser.from_result(case parser(contents) {
    Ok(#(result, _)) -> Ok(result)
    Error(err) -> Error(err)
  })
}
