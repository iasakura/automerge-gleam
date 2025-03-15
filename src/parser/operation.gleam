import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result.{try}
import parser/column
import parser/error
import parser/parser.{type Parser, do, ret, ret_error}
import parser/primitives
import parser/value

pub type Operation {
  Operation(
    object_id: Option(primitives.ObjectId),
    key: primitives.Key,
    // only exists in specification, but ref impl doesn't have it
    // id: primitives.OperationId,
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
        // 2, column.Actor -> {
        //   use col <- do(column.decode_actor_column())
        //   ret([ActorId(col), ..acc])
        // }
        // 2, column.Delta -> {
        //   use col <- do(column.decode_delta_column())
        //   ret([Counter(col), ..acc])
        // }
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

fn get_from_columns(columns: List(a), get: fn(a) -> Option(b)) -> Option(b) {
  list.find_map(columns, fn(column) {
    case get(column) {
      Some(column) -> Ok(column)
      None -> Error(Nil)
    }
  })
  |> option.from_result
}

pub fn decode_operations(
  column_metadata: List(column.ColumnMetadata),
  // for actor columns
  actor_id: BitArray,
  other_actors: List(primitives.ActorId),
) -> Parser(List(Operation)) {
  use columns <- do(decode_operation_columns(column_metadata))
  let object_actor_id =
    list.find_map(columns, fn(column) {
      case column {
        ObjectActorId(column) -> Ok(column)
        _ -> Error(Nil)
      }
    })
    |> option.from_result
  let object_counter =
    get_from_columns(columns, fn(column) {
      case column {
        ObjectCounter(column) -> Some(column)
        _ -> None
      }
    })
  let key_actor_id =
    get_from_columns(columns, fn(column) {
      case column {
        KeyActorId(column) -> Some(column)
        _ -> None
      }
    })
  let key_counter =
    get_from_columns(columns, fn(column) {
      case column {
        KeyCounter(column) -> Some(column)
        _ -> None
      }
    })
  let key_string =
    get_from_columns(columns, fn(column) {
      case column {
        KeyString(column) -> Some(column)
        _ -> None
      }
    })
  // let op_actor_id =
  //   get_from_columns(columns, fn(column) {
  //     case column {
  //       ActorId(column) -> Some(column)
  //       _ -> None
  //     }
  //   })
  // let counter =
  //   get_from_columns(columns, fn(column) {
  //     case column {
  //       Counter(column) -> Some(column)
  //       _ -> None
  //     }
  //   })
  let insert =
    get_from_columns(columns, fn(column) {
      case column {
        Insert(column) -> Some(column)
        _ -> None
      }
    })
  let action =
    get_from_columns(columns, fn(column) {
      case column {
        Action(column) -> Some(column)
        _ -> None
      }
    })
  let value_metadata =
    get_from_columns(columns, fn(column) {
      case column {
        ValueMetadata(column) -> Some(column)
        _ -> None
      }
    })
  let value =
    get_from_columns(columns, fn(column) {
      case column {
        Value(column) -> Some(column)
        _ -> None
      }
    })
  let predecessor_group =
    get_from_columns(columns, fn(column) {
      case column {
        PredecessorGroup(column) -> Some(column)
        _ -> None
      }
    })
  let predecessor_actor_id =
    get_from_columns(columns, fn(column) {
      case column {
        PredecessorActorId(column) -> Some(column)
        _ -> None
      }
    })
  let predecessor_counter =
    get_from_columns(columns, fn(column) {
      case column {
        PredecessorCounter(column) -> Some(column)
        _ -> None
      }
    })
  let successor_group =
    get_from_columns(columns, fn(column) {
      case column {
        SuccessorGroup(column) -> Some(column)
        _ -> None
      }
    })
  let successor_actor_id =
    get_from_columns(columns, fn(column) {
      case column {
        SuccessorActorId(column) -> Some(column)
        _ -> None
      }
    })
  let successor_counter =
    get_from_columns(columns, fn(column) {
      case column {
        SuccessorCounter(column) -> Some(column)
        _ -> None
      }
    })
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
    // op_actor_id,
    // counter,
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
  actor_idx: Int,
  actor_id: primitives.ActorId,
  other_actors: List(primitives.ActorId),
) -> Result(primitives.ActorId, error.ParseError) {
  case actor_idx {
    0 -> Ok(actor_id)
    n if n > 0 -> get_actor_id_from_other_actors(n - 1, other_actors)
    _ -> Error(error.InvalidActorIndex)
  }
}

fn get_actor_id_from_other_actors(
  actor_idx: Int,
  other_actors: List(primitives.ActorId),
) -> Result(primitives.ActorId, error.ParseError) {
  case actor_idx, other_actors {
    0, [actor_id, ..] -> Ok(actor_id)
    n, [_, ..other_actors] if n > 0 ->
      get_actor_id_from_other_actors(n - 1, other_actors)
    _, _ -> Error(error.InvalidActorIndex)
  }
}

fn to_cons(x: Option(List(a)), nil: a) -> Option(#(a, Option(List(a)))) {
  case x {
    Some([head, ..tail]) -> Some(#(head, Some(tail)))
    Some([]) -> None
    None -> Some(#(nil, None))
  }
}

fn to_nil(x: Option(List(a))) -> Bool {
  case x {
    Some([_, ..]) -> False
    Some([]) -> True
    None -> True
  }
}

fn split(x: Option(List(a)), nil: a, n: Int) -> #(List(a), Option(List(a))) {
  case x {
    Some(x) -> {
      let #(head, tail) = list.split(x, n)
      #(head, Some(tail))
    }
    None -> #(list.repeat(nil, n), None)
  }
}

fn columns_to_records(
  actor_id: BitArray,
  other_actors: List(primitives.ActorId),
  object_actor_ids: Option(column.ActorColumn),
  object_counters: Option(column.ULEBColumn),
  key_actor_ids: Option(column.ActorColumn),
  key_counters: Option(column.ULEBColumn),
  key_strings: Option(column.StringColumn),
  // op_actor_ids: Option(column.ActorColumn),
  // counters: Option(column.DeltaColumn),
  inserts: Option(column.BooleanColumn),
  actions: Option(column.ULEBColumn),
  value_metadatas: Option(column.ValueMetadataColumn),
  values: Option(column.ValueColumn),
  predecessor_groups: Option(column.GroupColumn),
  predecessor_actor_ids: Option(column.ActorColumn),
  predecessor_counters: Option(column.DeltaColumn),
  successor_groups: Option(column.GroupColumn),
  successor_actor_ids: Option(column.ActorColumn),
  successor_counters: Option(column.DeltaColumn),
  other_columns: List(#(column.ColumnMetadata, BitArray)),
) -> Result(List(Operation), error.ParseError) {
  case
    to_cons(object_actor_ids, None),
    to_cons(object_counters, None),
    to_cons(key_actor_ids, None),
    to_cons(key_counters, None),
    to_cons(key_strings, None),
    // to_cons(op_actor_ids, None),
    // to_cons(counters, 0),
    to_cons(inserts, False),
    to_cons(actions, None),
    // ?
    to_cons(value_metadatas, value.NullValueMetadata(0)),
    to_cons(values, primitives.Null),
    to_cons(predecessor_groups, None),
    to_cons(successor_groups, None)
  {
    Some(#(object_actor_id, object_actor_ids)),
      Some(#(object_counter, object_counters)),
      Some(#(key_actor_id, key_actor_ids)),
      Some(#(key_counter, key_counters)),
      Some(#(key_string, key_strings)),
      // Some(#(op_actor_id, op_actor_ids)),
      // Some(#(counter, counters)),
      Some(#(insert, inserts)),
      Some(#(action, actions)),
      Some(#(_value_metadata, value_metadatas)),
      Some(#(value, values)),
      Some(#(predecessor_group, predecessor_groups)),
      Some(#(successor_group, successor_groups))
    -> {
      use object_id <- try(case object_actor_id, object_counter {
        Some(object_actor_id), Some(object_counter) -> {
          use object_actor_id <- result.try(get_actor_id(
            object_actor_id,
            actor_id,
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
                actor_id,
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
      // use id <- try(case op_actor_id, counter {
      //   Some(op_actor_id), counter -> {
      //     use op_actor_id <- result.try(get_actor_id(op_actor_id, other_actors))
      //     Ok(primitives.OperationId(op_actor_id, counter))
      //   }
      //   _, _ -> Error(error.InvalidId)
      // })
      use #(predecessors, predecessor_actor_ids, predecessor_counters) <- try(case
        predecessor_group
      {
        None -> Ok(#(None, predecessor_actor_ids, predecessor_counters))
        Some(n) -> {
          let #(actor_ids, predecessor_actor_id) =
            split(predecessor_actor_ids, None, n)
          use Nil <- try(case list.length(actor_ids) == n {
            False -> Error(error.InvalidPredecessors)
            True -> Ok(Nil)
          })
          let #(counters, predecessor_counter) =
            split(predecessor_counters, 0, n)
          use Nil <- try(case list.length(counters) == n {
            False -> Error(error.InvalidPredecessors)
            True -> Ok(Nil)
          })
          use ids <- try(
            list.zip(actor_ids, counters)
            |> list.try_map(fn(pair) {
              let #(actor_idx, counter) = pair
              use actor_idx <- result.try(case actor_idx {
                Some(actor_id) -> Ok(actor_id)
                None -> Error(error.MissingPredecessorActorId)
              })
              use actor_id <- result.try(get_actor_id(
                actor_idx,
                actor_id,
                other_actors,
              ))
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
            split(successor_actor_ids, None, n)
          use Nil <- try(case list.length(actor_ids) == n {
            False -> Error(error.InvalidSuccessors)
            True -> Ok(Nil)
          })
          let #(counters, successor_counter) = split(successor_counters, 0, n)
          use Nil <- try(case list.length(counters) == n {
            False -> Error(error.InvalidSuccessors)
            True -> Ok(Nil)
          })
          use ids <- try(
            list.zip(actor_ids, counters)
            |> list.try_map(fn(pair) {
              let #(actor_idx, counter) = pair
              use actor_idx <- result.try(case actor_idx {
                Some(actor_id) -> Ok(actor_id)
                None -> Error(error.MissingSuccessorActorId)
              })
              use actor_id <- result.try(get_actor_id(
                actor_idx,
                actor_id,
                other_actors,
              ))
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
        // op_actor_ids,
        // counters,
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
          // id,
          insert,
          action,
          value,
          predecessors,
          successors,
        ),
        ..rest
      ])
    }
    _, _, _, _, _, _, _, _, _, _, _ -> {
      case
        to_nil(object_actor_ids),
        to_nil(object_counters),
        to_nil(key_actor_ids),
        to_nil(key_counters),
        to_nil(key_strings),
        // to_nil(op_actor_ids),
        // to_nil(counters),
        to_nil(inserts),
        to_nil(actions),
        // ?
        to_nil(value_metadatas),
        to_nil(values),
        to_nil(predecessor_groups),
        to_nil(successor_groups)
      {
        True, True, True, True, True, True, True, True, True, True, True ->
          Ok([])
        _, _, _, _, _, _, _, _, _, _, _ -> Error(error.InvalidOperationColumns)
      }
    }
  }
}
