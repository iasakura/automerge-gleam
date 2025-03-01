import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result.{try}
import parser/column
import parser/error
import parser/parser.{type Parser, do, ret, ret_error}
import parser/primitives

pub type Operation {
  Operation(
    object_id: Option(primitives.ObjectId),
    key: primitives.Key,
    id: primitives.OperationId,
    insert: Bool,
    action: primitives.Action,
    value: primitives.RawValue,
    predecessors: Option(List(primitives.OperationId)),
    successors: Option(List(primitives.OperationId)),
  )
}

pub type OperationColumns {
  ObjectActorId(column.ActorColumn)
  ObjectCounter(column.ULEBColumn)
  KeyActorId(column.ActorColumn)
  KeyCounter(column.ULEBColumn)
  KeyString(column.StringColumn)
  ActorId(column.ActorColumn)
  Counter(column.DeltaColumn)
  Insert(column.BooleanColumn)
  Action(column.ULEBColumn)
  ValueMetadata(column.ValueMetadataColumn)
  Value(column.ValueColumn)
  PredecessorGroup(column.GroupColumn)
  PredecessorActorId(column.ActorColumn)
  PredecessorCounter(column.DeltaColumn)
  SuccessorGroup(column.GroupColumn)
  SuccessorActorId(column.ActorColumn)
  SuccessorCounter(column.DeltaColumn)
  UnknownColumn(metadata: column.ColumnMetadata, data: column.UnknownColumn)
}

pub fn decode_operation_columns(
  column_metadata: List(column.ColumnMetadata),
) -> Parser(List(OperationColumns)) {
  use columns <- do(
    list.fold(column_metadata, ret([]), fn(acc, metadata) {
      use acc <- do(acc)
      use bytes <- do(parser.n_bytes(metadata.column_len))
      let parser = case
        metadata.column_spec.id,
        metadata.column_spec.column_type
      {
        0, column.Actor -> {
          use col <- do(column.decode_actor_column())
          ret([ObjectActorId(col), ..acc])
        }
        0, column.ULEB -> {
          use col <- do(column.decode_uleb_column())
          ret([ObjectCounter(col), ..acc])
        }
        1, column.Actor -> {
          use col <- do(column.decode_actor_column())
          ret([KeyActorId(col), ..acc])
        }
        1, column.ULEB -> {
          use col <- do(column.decode_uleb_column())
          ret([KeyCounter(col), ..acc])
        }
        1, column.String -> {
          use col <- do(column.decode_string_column())
          ret([KeyString(col), ..acc])
        }
        2, column.Actor -> {
          use col <- do(column.decode_actor_column())
          ret([ActorId(col), ..acc])
        }
        2, column.Delta -> {
          use col <- do(column.decode_delta_column())
          ret([Counter(col), ..acc])
        }
        3, column.Boolean -> {
          use col <- do(column.decode_boolean_column())
          ret([Insert(col), ..acc])
        }
        4, column.ULEB -> {
          use col <- do(column.decode_uleb_column())
          ret([Action(col), ..acc])
        }
        5, column.ValueMetadata -> {
          use col <- do(column.decode_value_metadata_column())
          ret([ValueMetadata(col), ..acc])
        }
        5, column.Value -> {
          case
            list.find_map(acc, fn(column) {
              case column {
                ValueMetadata(metadata) -> Ok(metadata)
                _ -> Error(Nil)
              }
            })
          {
            Ok(metadata) -> {
              use col <- do(column.decode_value_column(metadata))
              ret([Value(col), ..acc])
            }
            Error(Nil) -> {
              ret_error(error.MissingValueMetadata)
            }
          }
        }
        7, column.Group -> {
          use col <- do(column.decode_group_column())
          ret([PredecessorGroup(col), ..acc])
        }
        7, column.Actor -> {
          use col <- do(column.decode_actor_column())
          ret([PredecessorActorId(col), ..acc])
        }
        7, column.Delta -> {
          use col <- do(column.decode_delta_column())
          ret([PredecessorCounter(col), ..acc])
        }
        8, column.Group -> {
          use col <- do(column.decode_group_column())
          ret([SuccessorGroup(col), ..acc])
        }
        8, column.Actor -> {
          use col <- do(column.decode_actor_column())
          ret([SuccessorActorId(col), ..acc])
        }
        8, column.Delta -> {
          use col <- do(column.decode_delta_column())
          ret([SuccessorCounter(col), ..acc])
        }
        _, _ -> {
          use res <- do(parser.get_bit_array())
          ret([UnknownColumn(metadata, res), ..acc])
        }
      }
      case parser(bytes) {
        Ok(#(column, <<>>)) -> ret(column)
        Ok(#(_, _)) -> ret_error(error.InvalidColumnLength)
        Error(err) -> ret_error(err)
      }
    }),
  )
  ret(columns)
}

fn get_from_columns(
  columns: List(a),
  get: fn(a) -> Option(b),
  error: error.ParseError,
) -> Parser(b) {
  parser.from_result(
    list.find_map(columns, fn(column) {
      case get(column) {
        Some(column) -> Ok(column)
        None -> Error(Nil)
      }
    })
    |> result.map_error(fn(_) { error }),
  )
}

pub fn decode_operations(
  column_metadata: List(column.ColumnMetadata),
  // for actor columns
  actor_id: BitArray,
  other_actors: List(primitives.ActorId),
) -> Parser(List(Operation)) {
  use columns <- do(decode_operation_columns(column_metadata))
  use object_actor_id <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        ObjectActorId(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingObjectActorId,
  ))
  use object_counter <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        ObjectCounter(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingObjectCounter,
  ))
  use key_actor_id <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        KeyActorId(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingKeyActorId,
  ))
  use key_counter <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        KeyCounter(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingKeyCounter,
  ))
  use key_string <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        KeyString(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingKeyString,
  ))
  use op_actor_id <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        ActorId(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingActorId,
  ))
  use counter <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        Counter(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingCounter,
  ))
  use insert <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        Insert(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingInsert,
  ))
  use action <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        Action(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingAction,
  ))
  use value_metadata <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        ValueMetadata(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingValueMetadata,
  ))
  use value <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        Value(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingValue,
  ))
  use predecessor_group <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        PredecessorGroup(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingPredecessorGroup,
  ))
  use predecessor_actor_id <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        PredecessorActorId(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingPredecessorActorId,
  ))
  use predecessor_counter <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        PredecessorCounter(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingPredecessorCounter,
  ))
  use successor_group <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        SuccessorGroup(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingSuccessorGroup,
  ))
  use successor_actor_id <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        SuccessorActorId(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingSuccessorActorId,
  ))
  use successor_counter <- do(get_from_columns(
    columns,
    fn(column) {
      case column {
        SuccessorCounter(column) -> Some(column)
        _ -> None
      }
    },
    error.MissingSuccessorCounter,
  ))
  let other_columns =
    list.filter_map(columns, fn(column) {
      case column {
        UnknownColumn(metadata, data) -> Ok(#(metadata, data))
        _ -> Error(Nil)
      }
    })
  parser.from_result(columns_to_records(
    actor_id,
    other_actors,
    object_actor_id,
    object_counter,
    key_actor_id,
    key_counter,
    key_string,
    op_actor_id,
    counter,
    insert,
    action,
    value_metadata,
    value,
    predecessor_group,
    predecessor_actor_id,
    predecessor_counter,
    successor_group,
    successor_actor_id,
    successor_counter,
    other_columns,
  ))
}

fn get_actor_id(
  actor_id: Int,
  other_actors: List(primitives.ActorId),
) -> Result(primitives.ActorId, error.ParseError) {
  case actor_id, other_actors {
    0, [actor_id, ..] -> Ok(actor_id)
    n, [_, ..other_actors] if n > 0 -> get_actor_id(n - 1, other_actors)
    _, _ -> Error(error.InvalidActorIndex)
  }
}

fn columns_to_records(
  actor_id: BitArray,
  other_actors: List(primitives.ActorId),
  object_actor_ids: column.ActorColumn,
  object_counters: column.ULEBColumn,
  key_actor_ids: column.ActorColumn,
  key_counters: column.ULEBColumn,
  key_strings: column.StringColumn,
  op_actor_ids: column.ActorColumn,
  counters: column.DeltaColumn,
  inserts: column.BooleanColumn,
  actions: column.ULEBColumn,
  value_metadatas: column.ValueMetadataColumn,
  values: column.ValueColumn,
  predecessor_groups: column.GroupColumn,
  predecessor_actor_ids: column.ActorColumn,
  predecessor_counters: column.DeltaColumn,
  successor_groups: column.GroupColumn,
  successor_actor_ids: column.ActorColumn,
  successor_counters: column.DeltaColumn,
  other_columns: List(#(column.ColumnMetadata, BitArray)),
) -> Result(List(Operation), error.ParseError) {
  case
    object_actor_ids,
    object_counters,
    key_actor_ids,
    key_counters,
    key_strings,
    op_actor_ids,
    counters,
    inserts,
    actions,
    value_metadatas,
    values,
    predecessor_groups,
    successor_groups
  {
    [object_actor_id, ..object_actor_ids],
      [object_counter, ..object_counters],
      [key_actor_id, ..key_actor_ids],
      [key_counter, ..key_counters],
      [key_string, ..key_strings],
      [op_actor_id, ..op_actor_ids],
      [counter, ..counters],
      [insert, ..inserts],
      [action, ..actions],
      [value_metadata, ..value_metadatas],
      [value, ..values],
      [predecessor_group, ..predecessor_groups],
      [successor_group, ..successor_groups]
    -> {
      use object_id <- try(case object_actor_id, object_counter {
        Some(object_actor_id), Some(object_counter) -> {
          use object_actor_id <- result.try(get_actor_id(
            object_actor_id,
            other_actors,
          ))
          Ok(Some(primitives.object_id(object_actor_id, object_counter)))
        }
        None, None -> Ok(None)
        _, _ -> Error(error.MissingObjectActorId)
      })

      use key <- try(case key_string {
        Some(key_string) -> Ok(primitives.StringKey(key_string))
        None ->
          case key_actor_id, key_counter {
            Some(key_actor_id), Some(key_counter) -> {
              use key_actor_id <- result.try(get_actor_id(
                key_actor_id,
                other_actors,
              ))
              Ok(
                primitives.OperationKey(primitives.object_id(
                  key_actor_id,
                  key_counter,
                )),
              )
            }
            _, _ -> Error(error.InvalidKey)
          }
      })
      use id <- try(case op_actor_id, counter {
        Some(op_actor_id), counter -> {
          use op_actor_id <- result.try(get_actor_id(op_actor_id, other_actors))
          Ok(primitives.OperationId(op_actor_id, counter))
        }
        _, _ -> Error(error.InvalidKey)
      })
      use #(predecessors, predecessor_actor_ids, predecessor_counters) <- try(case
        predecessor_group
      {
        None -> Ok(#(None, predecessor_actor_ids, predecessor_counters))
        Some(n) -> {
          let #(actor_ids, predecessor_actor_id) =
            list.split(predecessor_actor_ids, n)
          use Nil <- try(case list.length(actor_ids) == n {
            False -> Error(error.InvalidOperationColumns)
            True -> Ok(Nil)
          })
          let #(counters, predecessor_counter) =
            list.split(predecessor_counters, n)
          use Nil <- try(case list.length(counters) == n {
            False -> Error(error.InvalidOperationColumns)
            True -> Ok(Nil)
          })
          use ids <- try(
            list.zip(actor_ids, counters)
            |> list.try_map(fn(pair) {
              let #(actor_id, counter) = pair
              use actor_id <- result.try(case actor_id {
                Some(actor_id) -> Ok(actor_id)
                None -> Error(error.MissingPredecessorActorId)
              })
              use actor_id <- result.try(get_actor_id(actor_id, other_actors))
              Ok(primitives.OperationId(actor_id, counter))
            }),
          )
          Ok(#(Some(ids), predecessor_actor_id, predecessor_counter))
        }
      })

      use action <- try(case action {
        Some(0) -> Ok(primitives.MakeMap)
        Some(1) -> Ok(primitives.Set)
        Some(2) -> Ok(primitives.MakeList)
        Some(3) -> Ok(primitives.Del)
        Some(4) -> Ok(primitives.MakeText)
        Some(5) -> Ok(primitives.Inc)
        _ -> Error(error.InvalidAction)
      })

      use #(successors, successor_actor_ids, successor_counters) <- try(case
        successor_group
      {
        None -> Ok(#(None, successor_actor_ids, successor_counters))
        Some(n) -> {
          let #(actor_ids, successor_actor_id) =
            list.split(successor_actor_ids, n)
          use Nil <- try(case list.length(actor_ids) == n {
            False -> Error(error.InvalidOperationColumns)
            True -> Ok(Nil)
          })
          let #(counters, successor_counter) = list.split(successor_counters, n)
          use Nil <- try(case list.length(counters) == n {
            False -> Error(error.InvalidOperationColumns)
            True -> Ok(Nil)
          })
          use ids <- try(
            list.zip(actor_ids, counters)
            |> list.try_map(fn(pair) {
              let #(actor_id, counter) = pair
              use actor_id <- result.try(case actor_id {
                Some(actor_id) -> Ok(actor_id)
                None -> Error(error.MissingSuccessorActorId)
              })
              use actor_id <- result.try(get_actor_id(actor_id, other_actors))
              Ok(primitives.OperationId(actor_id, counter))
            }),
          )
          Ok(#(Some(ids), successor_actor_id, successor_counter))
        }
      })
      use rest <- try(columns_to_records(
        actor_id,
        other_actors,
        object_actor_ids,
        object_counters,
        key_actor_ids,
        key_counters,
        key_strings,
        op_actor_ids,
        counters,
        inserts,
        actions,
        value_metadatas,
        values,
        predecessor_groups,
        predecessor_actor_ids,
        predecessor_counters,
        successor_groups,
        successor_actor_ids,
        successor_counters,
        other_columns,
      ))
      Ok([
        Operation(
          object_id,
          key,
          id,
          insert,
          action,
          value,
          predecessors,
          successors,
        ),
        ..rest
      ])
    }
    [], [], [], [], [], [], [], [], [], [], [], [], [] -> Ok([])
    _, _, _, _, _, _, _, _, _, _, _, _, _ ->
      Error(error.InvalidOperationColumns)
  }
}
