use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use redis_rs::resp_types::RespValue;
use redis_rs::storage::Storage;
use redis_rs::task_communication::Channels;
use redis_rs::replication::{ReplicationState, ReplicationStateHandle};
use redis_rs::{
    handle_blpop_cmd, handle_echo_cmd, handle_get_cmd, handle_incr_cmd, handle_info_cmd,
    handle_llen_cmd, handle_lpop_cmd, handle_lpush_cmd, handle_lrange_cmd, handle_ping_cmd,
    handle_replconf_cmd, handle_rpush_cmd, handle_set_cmd, handle_type_cmd,
    handle_xadd_cmd, handle_xrange_cmd, handle_xread_cmd,
};

fn make_storage() -> Storage {
    Arc::new(RwLock::new(HashMap::new()))
}

fn make_channels() -> Channels {
    Arc::new(RwLock::new(HashMap::new()))
}

fn make_repl_state() -> ReplicationStateHandle {
    Arc::new(RwLock::new(ReplicationState::new_master()))
}

fn cmd(parts: &[&str]) -> Vec<RespValue> {
    parts
        .iter()
        .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
        .collect()
}

fn array_strings(v: RespValue) -> Vec<String> {
    match v {
        RespValue::Array(deque) => deque
            .iter()
            .map(|e| match e {
                RespValue::BulkString(b) => String::from_utf8(b.clone()).unwrap(),
                RespValue::SimpleString(s) => s.clone(),
                other => panic!("unexpected element: {:?}", other),
            })
            .collect(),
        other => panic!("expected Array, got {:?}", other),
    }
}

mod resp_serialization {
    use super::*;

    fn round_trip(v: &RespValue) -> RespValue {
        RespValue::deserialize(&v.serialize()).unwrap()
    }

    #[test]
    fn simple_string() {
        let v = RespValue::SimpleString("PONG".into());
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn bulk_string() {
        let v = RespValue::BulkString(b"hello world".to_vec());
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn integer() {
        for n in [-1i64, 0, 1, 42, i64::MAX] {
            let v = RespValue::Integer(n);
            assert_eq!(round_trip(&v), v);
        }
    }

    #[test]
    fn null_bulk_string_wire_format() {
        assert_eq!(RespValue::NullBulkString.serialize(), b"$-1\r\n");
    }

    #[test]
    fn null_array_wire_format() {
        assert_eq!(RespValue::NullArray.serialize(), b"*-1\r\n");
    }

    #[test]
    fn nested_array() {
        use std::collections::VecDeque;
        let mut inner = VecDeque::new();
        inner.push_back(RespValue::BulkString(b"SET".to_vec()));
        inner.push_back(RespValue::BulkString(b"foo".to_vec()));
        inner.push_back(RespValue::BulkString(b"bar".to_vec()));
        let v = RespValue::Array(inner);
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn simple_error() {
        let v = RespValue::SimpleError("ERR something went wrong".into());
        assert_eq!(round_trip(&v), v);
    }
}

mod stream_id {
    use redis_rs::resp_types::StreamId;

    fn sid(ms: u64, seq: u64) -> StreamId {
        StreamId { milliseconds: Some(ms), sequence: Some(seq) }
    }

    #[test]
    fn ordering() {
        assert!(sid(0, 1) > sid(0, 0));
        assert!(sid(1, 0) > sid(0, 999));
        assert!(sid(1, 1) > sid(1, 0));
        assert_eq!(sid(5, 3), sid(5, 3));
    }

    #[test]
    fn display() {
        assert_eq!(sid(100, 5).to_string(), "100-5");
    }
}

mod ping {
    use super::*;

    #[test]
    fn returns_pong() {
        assert_eq!(handle_ping_cmd(), RespValue::SimpleString("PONG".into()));
    }

    #[test]
    fn stateless_repeated() {
        for _ in 0..3 {
            assert_eq!(handle_ping_cmd(), RespValue::SimpleString("PONG".into()));
        }
    }

    #[tokio::test]
    async fn concurrent_clients() {
        let handles: Vec<_> = (0..6)
            .map(|_| tokio::spawn(async { handle_ping_cmd() }))
            .collect();
        for h in handles {
            assert_eq!(h.await.unwrap(), RespValue::SimpleString("PONG".into()));
        }
    }
}

mod echo {
    use super::*;

    #[test]
    fn echoes_argument() {
        assert_eq!(
            handle_echo_cmd(&cmd(&["ECHO", "apple"])),
            RespValue::BulkString(b"apple".to_vec())
        );
    }

    #[test]
    fn preserves_spaces() {
        assert_eq!(
            handle_echo_cmd(&cmd(&["ECHO", "hello world"])),
            RespValue::BulkString(b"hello world".to_vec())
        );
    }
}

mod set_get {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn basic_set_then_get() {
        let s = make_storage();
        let r = make_repl_state();
        assert_eq!(
            handle_set_cmd(&cmd(&["SET", "orange", "blueberry"]), s.clone(), r).await,
            RespValue::SimpleString("OK".into())
        );
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "orange"]), s).await,
            RespValue::BulkString(b"blueberry".to_vec())
        );
    }

    #[tokio::test]
    async fn get_missing_key_returns_null() {
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "nonexistent"]), make_storage()).await,
            RespValue::NullBulkString
        );
    }

    #[tokio::test]
    async fn set_overwrites_existing_key() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "k", "v1"]), s.clone(), r.clone()).await;
        handle_set_cmd(&cmd(&["SET", "k", "v2"]), s.clone(), r).await;
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "k"]), s).await,
            RespValue::BulkString(b"v2".to_vec())
        );
    }

    #[tokio::test]
    async fn px_expiry() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "apple", "strawberry", "PX", "100"]), s.clone(), r).await;
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "apple"]), s.clone()).await,
            RespValue::BulkString(b"strawberry".to_vec())
        );
        sleep(Duration::from_millis(110)).await;
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "apple"]), s).await,
            RespValue::NullBulkString
        );
    }

    #[tokio::test]
    async fn ex_expiry_alive_within_window() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "key", "value", "EX", "10"]), s.clone(), r).await;
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "key"]), s).await,
            RespValue::BulkString(b"value".to_vec())
        );
    }
}

mod type_cmd {
    use super::*;

    #[tokio::test]
    async fn string_type() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "blueberry", "raspberry"]), s.clone(), r).await;
        assert_eq!(
            handle_type_cmd(&cmd(&["TYPE", "blueberry"]), s).await,
            RespValue::SimpleString("string".into())
        );
    }

    #[tokio::test]
    async fn missing_key_is_none() {
        assert_eq!(
            handle_type_cmd(&cmd(&["TYPE", "missing_key"]), make_storage()).await,
            RespValue::SimpleString("none".into())
        );
    }

    #[tokio::test]
    async fn stream_type() {
        let s = make_storage();
        let c = make_channels();
        handle_xadd_cmd(&cmd(&["XADD", "raspberry", "0-1", "foo", "bar"]), s.clone(), c).await;
        assert_eq!(
            handle_type_cmd(&cmd(&["TYPE", "raspberry"]), s).await,
            RespValue::SimpleString("stream".into())
        );
    }
}

mod push {
    use super::*;

    #[tokio::test]
    async fn rpush_creates_list_of_one() {
        assert_eq!(
            handle_rpush_cmd(&cmd(&["RPUSH", "grape", "orange"]), make_storage(), make_channels()).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn rpush_increments_length() {
        let s = make_storage();
        let c = make_channels();
        for (val, expected) in [("blueberry", 1i64), ("pineapple", 2), ("mango", 3)] {
            assert_eq!(
                handle_rpush_cmd(&cmd(&["RPUSH", "raspberry", val]), s.clone(), c.clone()).await,
                RespValue::Integer(expected)
            );
        }
    }

    #[tokio::test]
    async fn rpush_multiple_values() {
        let s = make_storage();
        let c = make_channels();
        assert_eq!(
            handle_rpush_cmd(&cmd(&["RPUSH", "mango", "strawberry", "pear"]), s.clone(), c.clone()).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            handle_rpush_cmd(&cmd(&["RPUSH", "mango", "raspberry", "strawberry", "blueberry"]), s, c).await,
            RespValue::Integer(5)
        );
    }

    #[tokio::test]
    async fn lpush_prepends_in_order() {
        let s = make_storage();
        handle_lpush_cmd(&cmd(&["LPUSH", "pear", "raspberry"]), s.clone()).await;
        handle_lpush_cmd(&cmd(&["LPUSH", "pear", "mango", "orange"]), s.clone()).await;
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "pear", "0", "-1"]), s).await),
            vec!["orange", "mango", "raspberry"]
        );
    }
}

mod lrange {
    use super::*;

    async fn build_list(key: &str, vals: &[&str]) -> Storage {
        let s = make_storage();
        let c = make_channels();
        let mut args = vec!["RPUSH", key];
        args.extend_from_slice(vals);
        handle_rpush_cmd(&cmd(&args), s.clone(), c).await;
        s
    }

    #[tokio::test]
    async fn positive_indexes() {
        let s = build_list("pineapple", &["raspberry", "blueberry", "grape", "pineapple", "orange"]).await;
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "pineapple", "0", "3"]), s.clone()).await),
            vec!["raspberry", "blueberry", "grape", "pineapple"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "pineapple", "3", "4"]), s.clone()).await),
            vec!["pineapple", "orange"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "pineapple", "0", "10"]), s.clone()).await),
            vec!["raspberry", "blueberry", "grape", "pineapple", "orange"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "pineapple", "1", "0"]), s.clone()).await),
            Vec::<String>::new()
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "missing_key", "0", "1"]), s).await),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn negative_indexes() {
        let s = build_list("strawberry", &["mango", "orange", "blueberry", "banana", "grape"]).await;
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "strawberry", "0", "-3"]), s.clone()).await),
            vec!["mango", "orange", "blueberry"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "strawberry", "-3", "-1"]), s.clone()).await),
            vec!["blueberry", "banana", "grape"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "strawberry", "0", "-1"]), s.clone()).await),
            vec!["mango", "orange", "blueberry", "banana", "grape"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "strawberry", "-1", "-2"]), s.clone()).await),
            Vec::<String>::new()
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "strawberry", "-6", "-1"]), s).await),
            vec!["mango", "orange", "blueberry", "banana", "grape"]
        );
    }
}

mod lpop {
    use super::*;

    async fn build_list(key: &str, vals: &[&str]) -> Storage {
        let s = make_storage();
        let c = make_channels();
        let mut args = vec!["RPUSH", key];
        args.extend_from_slice(vals);
        handle_rpush_cmd(&cmd(&args), s.clone(), c).await;
        s
    }

    #[tokio::test]
    async fn single_pop() {
        let s = build_list(
            "raspberry",
            &["mango", "orange", "pineapple", "raspberry", "pear", "apple", "strawberry"],
        )
        .await;
        assert_eq!(
            handle_lpop_cmd(&cmd(&["LPOP", "raspberry"]), s.clone()).await,
            RespValue::BulkString(b"mango".to_vec())
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "raspberry", "0", "-1"]), s).await),
            vec!["orange", "pineapple", "raspberry", "pear", "apple", "strawberry"]
        );
    }

    #[tokio::test]
    async fn pop_with_count() {
        let s = build_list("mango", &["pear", "pineapple", "mango", "apple", "orange", "raspberry"]).await;
        assert_eq!(
            array_strings(handle_lpop_cmd(&cmd(&["LPOP", "mango", "3"]), s.clone()).await),
            vec!["pear", "pineapple", "mango"]
        );
        assert_eq!(
            array_strings(handle_lrange_cmd(&cmd(&["LRANGE", "mango", "0", "-1"]), s).await),
            vec!["apple", "orange", "raspberry"]
        );
    }

    #[tokio::test]
    async fn missing_key_returns_null() {
        assert_eq!(
            handle_lpop_cmd(&cmd(&["LPOP", "ghost"]), make_storage()).await,
            RespValue::NullBulkString
        );
    }
}

mod llen {
    use super::*;

    #[tokio::test]
    async fn existing_and_missing() {
        let s = make_storage();
        let c = make_channels();
        handle_rpush_cmd(
            &cmd(&["RPUSH", "orange", "apple", "blueberry", "strawberry", "mango", "pineapple"]),
            s.clone(),
            c,
        )
        .await;
        assert_eq!(handle_llen_cmd(&cmd(&["LLEN", "orange"]), s.clone()).await, RespValue::Integer(5));
        assert_eq!(handle_llen_cmd(&cmd(&["LLEN", "missing_key"]), s).await, RespValue::Integer(0));
    }
}

mod blpop {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn timeout_returns_null_array() {
        assert_eq!(
            handle_blpop_cmd(&cmd(&["BLPOP", "banana", "0.1"]), make_storage(), make_channels()).await,
            RespValue::NullArray
        );
    }

    #[tokio::test]
    async fn wakes_on_rpush() {
        let s = make_storage();
        let c = make_channels();
        let (sc, cc) = (s.clone(), c.clone());
        let waiter = tokio::spawn(async move {
            handle_blpop_cmd(&cmd(&["BLPOP", "grape", "1.0"]), sc, cc).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_rpush_cmd(&cmd(&["RPUSH", "grape", "orange"]), s, c).await;
        let resp = waiter.await.unwrap();
        assert_eq!(
            resp,
            RespValue::Array({
                let mut d = std::collections::VecDeque::new();
                d.push_back(RespValue::BulkString(b"grape".to_vec()));
                d.push_back(RespValue::BulkString(b"orange".to_vec()));
                d
            })
        );
    }

    #[tokio::test]
    async fn only_one_waiter_woken_per_push() {
        let s = make_storage();
        let c = make_channels();
        let (s1, c1) = (s.clone(), c.clone());
        let (s2, c2) = (s.clone(), c.clone());
        let w1 = tokio::spawn(async move {
            handle_blpop_cmd(&cmd(&["BLPOP", "grape", "1.0"]), s1, c1).await
        });
        let w2 = tokio::spawn(async move {
            handle_blpop_cmd(&cmd(&["BLPOP", "grape", "0.3"]), s2, c2).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_rpush_cmd(&cmd(&["RPUSH", "grape", "orange"]), s, c).await;
        assert!(matches!(w1.await.unwrap(), RespValue::Array(_)));
        assert_eq!(w2.await.unwrap(), RespValue::NullArray);
    }
}

mod xadd {
    use super::*;

    #[tokio::test]
    async fn explicit_id_is_returned() {
        assert_eq!(
            handle_xadd_cmd(&cmd(&["XADD", "raspberry", "0-1", "foo", "bar"]), make_storage(), make_channels()).await,
            RespValue::BulkString(b"0-1".to_vec())
        );
    }

    #[tokio::test]
    async fn rejects_zero_zero_id() {
        let resp = handle_xadd_cmd(
            &cmd(&["XADD", "grape", "0-0", "a", "b"]),
            make_storage(),
            make_channels(),
        )
        .await;
        assert!(matches!(&resp, RespValue::SimpleError(s) if s.contains("greater than 0-0")));
    }

    #[tokio::test]
    async fn rejects_non_monotonic_ids() {
        let s = make_storage();
        let c = make_channels();
        handle_xadd_cmd(&cmd(&["XADD", "grape", "1-1", "a", "b"]), s.clone(), c.clone()).await;
        handle_xadd_cmd(&cmd(&["XADD", "grape", "1-2", "a", "b"]), s.clone(), c.clone()).await;
        let r = handle_xadd_cmd(&cmd(&["XADD", "grape", "1-2", "a", "b"]), s.clone(), c.clone()).await;
        assert!(matches!(&r, RespValue::SimpleError(s) if s.contains("equal or smaller")));
        let r = handle_xadd_cmd(&cmd(&["XADD", "grape", "0-3", "a", "b"]), s, c).await;
        assert!(matches!(&r, RespValue::SimpleError(s) if s.contains("equal or smaller")));
    }

    #[tokio::test]
    async fn partial_auto_sequence() {
        let s = make_storage();
        let c = make_channels();
        assert_eq!(
            handle_xadd_cmd(&cmd(&["XADD", "r", "0-*", "a", "b"]), s.clone(), c.clone()).await,
            RespValue::BulkString(b"0-1".to_vec())
        );
        assert_eq!(
            handle_xadd_cmd(&cmd(&["XADD", "r", "1-*", "a", "b"]), s.clone(), c.clone()).await,
            RespValue::BulkString(b"1-0".to_vec())
        );
        assert_eq!(
            handle_xadd_cmd(&cmd(&["XADD", "r", "1-*", "a", "b"]), s, c).await,
            RespValue::BulkString(b"1-1".to_vec())
        );
    }

    #[tokio::test]
    async fn full_auto_id_format() {
        let resp = handle_xadd_cmd(
            &cmd(&["XADD", "pineapple", "*", "foo", "bar"]),
            make_storage(),
            make_channels(),
        )
        .await;
        if let RespValue::BulkString(bytes) = resp {
            let id = String::from_utf8(bytes).unwrap();
            let parts: Vec<&str> = id.split('-').collect();
            assert_eq!(parts.len(), 2);
            let ms: u64 = parts[0].parse().expect("ms must be a number");
            let seq: u64 = parts[1].parse().expect("seq must be a number");
            assert!(ms > 0);
            assert_eq!(seq, 0);
        } else {
            panic!("expected BulkString for auto-generated ID");
        }
    }
}

mod xrange {
    use super::*;
    use std::collections::VecDeque;

    async fn populate(s: &Storage, c: &Channels, key: &str, entries: &[(&str, &str, &str)]) {
        for (id, f, v) in entries {
            handle_xadd_cmd(&cmd(&["XADD", key, id, f, v]), s.clone(), c.clone()).await;
        }
    }

    fn entry_count(v: RespValue) -> usize {
        match v {
            RespValue::Array(d) => d.len(),
            _ => panic!("expected array"),
        }
    }

    fn first_entry_id(v: &RespValue) -> String {
        match v {
            RespValue::Array(outer) => match &outer[0] {
                RespValue::Array(entry) => match &entry[0] {
                    RespValue::BulkString(b) => String::from_utf8(b.clone()).unwrap(),
                    _ => panic!(),
                },
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn explicit_bounds() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "grape", &[("0-1", "grape", "pear"), ("0-2", "grape", "orange"), ("0-3", "pineapple", "mango")]).await;
        let resp = handle_xrange_cmd(&cmd(&["XRANGE", "grape", "0-2", "0-3"]), s).await;
        assert_eq!(entry_count(resp.clone()), 2);
        assert_eq!(first_entry_id(&resp), "0-2");
    }

    #[tokio::test]
    async fn minus_lower_bound() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "strawberry", &[("0-1", "a", "b"), ("0-2", "c", "d"), ("0-3", "e", "f"), ("0-4", "g", "h")]).await;
        assert_eq!(
            entry_count(handle_xrange_cmd(&cmd(&["XRANGE", "strawberry", "-", "0-3"]), s).await),
            3
        );
    }

    #[tokio::test]
    async fn plus_upper_bound() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "pear", &[("0-1", "a", "b"), ("0-2", "c", "d"), ("0-3", "e", "f"), ("0-4", "g", "h")]).await;
        assert_eq!(
            entry_count(handle_xrange_cmd(&cmd(&["XRANGE", "pear", "0-1", "+"]), s).await),
            4
        );
    }

    #[tokio::test]
    async fn missing_key_returns_empty() {
        assert_eq!(
            handle_xrange_cmd(&cmd(&["XRANGE", "ghost", "-", "+"]), make_storage()).await,
            RespValue::Array(VecDeque::new())
        );
    }
}

mod xread {
    use super::*;
    use std::time::Duration;

    async fn populate(s: &Storage, c: &Channels, key: &str, entries: &[(&str, &str, &str)]) {
        for (id, f, v) in entries {
            handle_xadd_cmd(&cmd(&["XADD", key, id, f, v]), s.clone(), c.clone()).await;
        }
    }

    fn outer_len(v: &RespValue) -> usize {
        match v {
            RespValue::Array(d) => d.len(),
            _ => panic!("expected array"),
        }
    }

    fn first_entry_id_in_stream(v: &RespValue) -> String {
        match v {
            RespValue::Array(outer) => match &outer[0] {
                RespValue::Array(stream) => match &stream[1] {
                    RespValue::Array(entries) => match &entries[0] {
                        RespValue::Array(entry) => match &entry[0] {
                            RespValue::BulkString(b) => String::from_utf8(b.clone()).unwrap(),
                            _ => panic!(),
                        },
                        _ => panic!(),
                    },
                    _ => panic!(),
                },
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn single_stream() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "banana", &[("0-1", "temperature", "18")]).await;
        let resp = handle_xread_cmd(&cmd(&["XREAD", "streams", "banana", "0-0"]), s, c).await;
        assert_eq!(outer_len(&resp), 1);
    }

    #[tokio::test]
    async fn multiple_streams() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "grape", &[("0-1", "temperature", "0")]).await;
        populate(&s, &c, "banana", &[("0-2", "humidity", "1")]).await;
        let resp = handle_xread_cmd(
            &cmd(&["XREAD", "streams", "grape", "banana", "0-0", "0-1"]),
            s, c,
        )
        .await;
        assert_eq!(outer_len(&resp), 2);
    }

    #[tokio::test]
    async fn block_wakes_on_xadd() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "pineapple", &[("0-1", "temperature", "15")]).await;
        let (sc, cc) = (s.clone(), c.clone());
        let waiter = tokio::spawn(async move {
            handle_xread_cmd(&cmd(&["XREAD", "block", "1000", "streams", "pineapple", "0-1"]), sc, cc).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_xadd_cmd(&cmd(&["XADD", "pineapple", "0-2", "temperature", "31"]), s, c).await;
        assert_eq!(first_entry_id_in_stream(&waiter.await.unwrap()), "0-2");
    }

    #[tokio::test]
    async fn block_timeout_returns_null() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "pineapple", &[("0-2", "temperature", "31")]).await;
        assert_eq!(
            handle_xread_cmd(&cmd(&["XREAD", "block", "100", "streams", "pineapple", "0-2"]), s, c).await,
            RespValue::NullArray
        );
    }

    #[tokio::test]
    async fn block_dollar_reads_only_future_entries() {
        let s = make_storage();
        let c = make_channels();
        populate(&s, &c, "strawberry", &[("0-1", "temperature", "69")]).await;
        let (sc, cc) = (s.clone(), c.clone());
        let waiter = tokio::spawn(async move {
            handle_xread_cmd(&cmd(&["XREAD", "block", "1000", "streams", "strawberry", "$"]), sc, cc).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_xadd_cmd(&cmd(&["XADD", "strawberry", "0-2", "temperature", "12"]), s, c).await;
        assert_eq!(first_entry_id_in_stream(&waiter.await.unwrap()), "0-2");
    }
}

mod incr {
    use super::*;

    #[tokio::test]
    async fn increments_from_zero_when_missing() {
        let s = make_storage();
        assert_eq!(handle_incr_cmd(&cmd(&["INCR", "pear"]), s.clone()).await, RespValue::Integer(1));
        assert_eq!(handle_incr_cmd(&cmd(&["INCR", "pear"]), s.clone()).await, RespValue::Integer(2));
        assert_eq!(
            handle_get_cmd(&cmd(&["GET", "pear"]), s).await,
            RespValue::BulkString(b"2".to_vec())
        );
    }

    #[tokio::test]
    async fn increments_existing_integer_key() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "mango", "34"]), s.clone(), r).await;
        assert_eq!(handle_incr_cmd(&cmd(&["INCR", "mango"]), s).await, RespValue::Integer(35));
    }

    #[tokio::test]
    async fn errors_on_non_integer_value() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "blueberry", "apple"]), s.clone(), r).await;
        assert_eq!(
            handle_incr_cmd(&cmd(&["INCR", "blueberry"]), s).await,
            RespValue::SimpleError("ERR value is not an integer or out of range".into())
        );
    }
}

mod info {
    use super::*;

    fn extract_field(resp: &RespValue, field: &str) -> Option<String> {
        if let RespValue::BulkString(bytes) = resp {
            let text = String::from_utf8(bytes.clone()).unwrap();
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix(&format!("{}:", field)) {
                    return Some(rest.trim().to_string());
                }
            }
        }
        None
    }

    #[tokio::test]
    async fn master_role_and_offset() {
        let s = make_storage();
        let r = make_repl_state();
        let resp = handle_info_cmd(&cmd(&["INFO", "replication"]), s, r).await;
        assert_eq!(extract_field(&resp, "role").as_deref(), Some("master"));
        assert_eq!(extract_field(&resp, "master_repl_offset").as_deref(), Some("0"));
        assert_eq!(extract_field(&resp, "master_replid").unwrap().len(), 40);
    }

    #[tokio::test]
    async fn replica_role() {
        use redis_rs::replication::ReplicaInfo;
        let s = make_storage();
        let info = ReplicaInfo { m_host: "127.0.0.1".into(), m_port: 6379 };
        let r: ReplicationStateHandle = Arc::new(RwLock::new(ReplicationState::new_replica(info)));
        let resp = handle_info_cmd(&cmd(&["INFO", "replication"]), s, r).await;
        assert_eq!(extract_field(&resp, "role").as_deref(), Some("slave"));
    }
}

mod replconf {
    use super::*;

    #[tokio::test]
    async fn listening_port_returns_ok() {
        let resp = handle_replconf_cmd(
            &cmd(&["REPLCONF", "listening-port", "6380"]),
            make_storage(), make_repl_state(),
        ).await;
        assert_eq!(resp, RespValue::SimpleString("OK".into()));
    }

    #[tokio::test]
    async fn capa_psync2_returns_ok() {
        let resp = handle_replconf_cmd(
            &cmd(&["REPLCONF", "capa", "psync2"]),
            make_storage(), make_repl_state(),
        ).await;
        assert_eq!(resp, RespValue::SimpleString("OK".into()));
    }

    #[tokio::test]
    async fn getack_returns_replconf_ack_offset() {
        let resp = handle_replconf_cmd(
            &cmd(&["REPLCONF", "GETACK", "*"]),
            make_storage(), make_repl_state(),
        ).await;
        match &resp {
            RespValue::Array(deque) => {
                assert_eq!(deque.len(), 3);
                assert_eq!(deque[0], RespValue::BulkString(b"REPLCONF".to_vec()));
                assert_eq!(deque[1], RespValue::BulkString(b"ACK".to_vec()));
                if let RespValue::BulkString(b) = &deque[2] {
                    String::from_utf8(b.clone()).unwrap().parse::<u64>().unwrap();
                } else {
                    panic!("expected BulkString offset");
                }
            }
            _ => panic!("expected Array"),
        }
    }

    #[tokio::test]
    async fn getack_reflects_replica_offset() {
        use redis_rs::replication::ReplicaInfo;
        let info = ReplicaInfo { m_host: "127.0.0.1".into(), m_port: 6379 };
        let r: ReplicationStateHandle = Arc::new(RwLock::new(ReplicationState::new_replica(info)));
        r.write().await.repl_offset = 31;
        let resp = handle_replconf_cmd(
            &cmd(&["REPLCONF", "GETACK", "*"]),
            make_storage(), r,
        ).await;
        if let RespValue::Array(deque) = &resp {
            if let RespValue::BulkString(b) = &deque[2] {
                assert_eq!(String::from_utf8(b.clone()).unwrap(), "31");
            }
        }
    }
}

mod replication_state {
    use redis_rs::replication::{NodeRole, ReplicaInfo, ReplicationState};

    #[test]
    fn master_starts_with_unique_replid() {
        let a = ReplicationState::new_master();
        let b = ReplicationState::new_master();
        assert_ne!(a.repl_id, b.repl_id);
        assert_eq!(a.repl_id.len(), 40);
    }

    #[test]
    fn master_and_replica_start_at_zero_offset() {
        let info = ReplicaInfo { m_host: "127.0.0.1".into(), m_port: 6379 };
        assert_eq!(ReplicationState::new_master().repl_offset, 0);
        assert_eq!(ReplicationState::new_replica(info).repl_offset, 0);
    }

    #[test]
    fn replica_stores_master_info() {
        let info = ReplicaInfo { m_host: "localhost".into(), m_port: 6380 };
        let state = ReplicationState::new_replica(info.clone());
        match state.role {
            NodeRole::Replica { info: stored } => assert_eq!(stored, info),
            NodeRole::Master { .. } => panic!("expected Replica role"),
        }
    }
}

mod set_propagation {
    use super::*;

    #[tokio::test]
    async fn offset_advances_after_set() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "foo", "123"]), s.clone(), r.clone()).await;
        assert_eq!(r.read().await.repl_offset, 31);
    }

    #[tokio::test]
    async fn offset_advances_cumulatively() {
        let s = make_storage();
        let r = make_repl_state();
        handle_set_cmd(&cmd(&["SET", "foo", "123"]), s.clone(), r.clone()).await;
        let after_first = r.read().await.repl_offset;
        handle_set_cmd(&cmd(&["SET", "baz", "789"]), s.clone(), r.clone()).await;
        assert!(r.read().await.repl_offset > after_first);
    }
}