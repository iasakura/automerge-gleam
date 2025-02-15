import gleam/bit_array
import gleam/crypto
import gleam/result.{try}
import parser/change
import parser/document
import parser/error
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
  actual == sum
}

pub fn decode_chunk(
  data: BitArray,
) -> Result(#(Chunk, BitArray), error.ParseError) {
  use rest <- try(case data {
    <<0x85, 0x6f, 0x4a, 0x83, rest:bits>> -> Ok(rest)
    _ -> Error(error.InvalidMagicNumber)
  })

  use #(sum, rest) <- try(case rest {
    <<checksum:bits-size(32), rest:bits>> -> Ok(#(checksum, rest))
    _ -> Error(error.InvalidEOF)
  })

  use #(chunk_type, rest) <- try(case rest {
    <<chunk_type:size(8), rest:bits>> -> {
      use chunk_type <- try(case chunk_type {
        0 | 1 | 2 -> Ok(chunk_type)
        _ -> Error(error.InvalidChunkType)
      })
      Ok(#(chunk_type, rest))
    }
    _ -> Error(error.InvalidEOF)
  })

  use #(chunk_length, rest) <- try(var_int.decode_uint(rest))

  let chunk_length_in_bits = chunk_length * 8
  use #(contents, rest) <- try(case rest {
    <<contents:size(chunk_length_in_bits)-bits, rest:bits>> ->
      Ok(#(contents, rest))
    _ -> Error(error.InvalidEOF)
  })

  let len = bit_array.byte_size(data) - bit_array.byte_size(rest) - 4 - 4
  use checksum_computed <- try(case data {
    <<_:size(32), _:size(32), computed:bits-size(len), _>> -> Ok(computed)
    _ -> Error(error.InternalError)
  })

  use #() <- try(case validate_checksum(checksum_computed, sum) {
    True -> Ok(#())
    False -> Error(error.InvalidCheckSum)
  })

  case chunk_type {
    0 -> {
      use document <- try(case document.decode_document(contents) {
        Ok(#(document, <<>>)) -> Ok(document)
        Ok(#(_, _)) -> Error(error.InvalidChunkLength)
        Error(e) -> Error(e)
      })
      Ok(#(DocumentChunk(document), rest))
    }
    1 -> {
      use change <- try(case change.decode_change(contents) {
        Ok(#(change, <<>>)) -> Ok(change)
        Ok(#(_, _)) -> Error(error.InvalidChunkLength)
        Error(e) -> Error(e)
      })
      Ok(#(ChangeChunk(change), rest))
    }
    2 -> {
      use change <- try(case change.decode_compressed_change(contents) {
        Ok(#(change, <<>>)) -> Ok(change)
        Ok(#(_, _)) -> Error(error.InvalidChunkLength)
        Error(e) -> Error(e)
      })
      Ok(#(CompressedChangeChunk(change), rest))
    }
    _ -> Error(error.InternalError)
  }
}
