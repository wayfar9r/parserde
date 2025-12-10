mod builder;
mod error;
mod formats;
mod record;
mod result;

pub use record::{Record, fields};

use formats::{bin, csv, txt};

pub use builder::{build_reader, build_serializer, build_writer};

pub use error::{RecordProduceError, RecordSerializeError};
pub use result::{RecordProduceResult, RecordSerializeResult};
