#![warn(unreachable_pub)]
#![warn(missing_docs)]

//! A simple library with utilities for
//! parsing, serializing, converting and comparing data
//! in csv, bin and txt formats.

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
