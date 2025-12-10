use crate::error::{
    FieldParseError, ReaderCreateError, RecordParseError, RecordProduceError, RecordReadError,
    RecordSerializeError, RecordWriteError,
};

/// Result of read and parse operation
pub type RecordProduceResult<T> = Result<T, RecordProduceError>;
/// Result of serialization data into bytes
pub type RecordSerializeResult<T> = Result<T, RecordSerializeError>;
pub type FieldParseResult<T> = Result<T, FieldParseError>;
pub type RecordParseResult<T> = Result<T, RecordParseError>;
pub type RecordReadResult<T> = Result<T, RecordReadError>;
pub type ReaderCreateResult<T> = Result<T, ReaderCreateError>;
pub type RecordWriteResult<T> = Result<T, RecordWriteError>;
