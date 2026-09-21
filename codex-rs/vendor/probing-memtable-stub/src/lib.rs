//! No-op stand-in for `probing-memtable` 0.2.5 (Unix shm). Enough for pulsing-actor to compile.

#[derive(Debug)]
pub enum DType {
    Str,
    I64,
}

#[derive(Debug, Default)]
pub struct Schema {
    _cols: Vec<(String, DType)>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn col(mut self, name: impl Into<String>, dtype: DType) -> Self {
        self._cols.push((name.into(), dtype));
        self
    }
}

#[derive(Debug)]
pub enum Value<'a> {
    Str(&'a str),
    I64(i64),
}

pub mod discover {
    use super::{Schema, Value};

    #[derive(Debug)]
    pub struct ExposedHashTable;

    impl ExposedHashTable {
        pub fn create(
            _name: &str,
            _buckets: usize,
            _arena: usize,
            _flags: u32,
        ) -> Result<Self, String> {
            Ok(Self)
        }

        pub fn writer(&mut self) -> HashWriter<'_> {
            HashWriter {
                _marker: std::marker::PhantomData,
            }
        }
    }

    pub struct HashWriter<'a> {
        _marker: std::marker::PhantomData<&'a mut ExposedHashTable>,
    }

    impl HashWriter<'_> {
        pub fn insert(&mut self, _key: &str, _val: &Value<'_>) -> Result<(), String> {
            Ok(())
        }

        pub fn remove(&mut self, _key: &str) {}
    }

    #[derive(Debug)]
    pub struct ExposedTable;

    impl ExposedTable {
        pub fn create(
            _name: &str,
            _schema: &Schema,
            _capacity: usize,
            _extra: u32,
        ) -> Result<Self, String> {
            Ok(Self)
        }

        pub fn push_row(&mut self, _row: &[Value<'_>]) {}
    }
}
