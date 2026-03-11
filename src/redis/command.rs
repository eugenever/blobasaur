use bytes::Bytes;

use crate::redis::stream::{
    error::{Error, Result},
    protocol::RespValue,
    storage::Store,
};

pub fn cmd_del(store: &Store, args: &[Bytes], use_lazy: bool) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("DEL"));
    }
    let count: i64 = if use_lazy {
        args.iter()
            .map(|k| if store.lazy_del(k) { 1 } else { 0 })
            .sum()
    } else {
        args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum()
    };
    Ok(RespValue::integer(count))
}
