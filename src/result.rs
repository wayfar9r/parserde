use crate::error::{
    FieldParseError, ReaderCreateError, RecordParseError, RecordProduceError, RecordReadError,
    RecordSerializeError, RecordWriteError,
};

/// Result of read and parse operation
pub type RecordProduceResult<T> = Result<T, RecordProduceError>;
/// Result of serialization data into bytes
pub type RecordSerializeResult<T> = Result<T, RecordSerializeError>;
pub(crate) type FieldParseResult<T> = Result<T, FieldParseError>;
pub(crate) type RecordParseResult<T> = Result<T, RecordParseError>;
pub(crate) type RecordReadResult<T> = Result<T, RecordReadError>;
pub(crate) type ReaderCreateResult<T> = Result<T, ReaderCreateError>;
pub(crate) type RecordWriteResult<T> = Result<T, RecordWriteError>;
