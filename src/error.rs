use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub struct FieldParseError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for FieldParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.text)
    }
}

impl Error for FieldParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

#[derive(Debug)]
pub(crate) struct RecordReadError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for RecordReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.text)
    }
}

impl Error for RecordReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

/// An error that occurs on converting structur to bytes
#[derive(Debug)]
pub struct RecordSerializeError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for RecordSerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. source {}",
            self.text,
            match &self.source {
                Some(err) => err.to_string(),
                None => "none".into(),
            }
        )
    }
}

/// An error that occures while reading data or parsing data
#[derive(Debug)]
pub struct RecordProduceError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for RecordProduceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. source {}",
            self.text,
            match &self.source {
                Some(err) => err.to_string(),
                None => "none".into(),
            }
        )
    }
}

impl Error for RecordProduceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

#[derive(Debug)]
pub(crate) struct RecordParseError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for RecordParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. source {}",
            self.text,
            match &self.source {
                Some(err) => err.to_string(),
                None => "none".into(),
            }
        )
    }
}

impl Error for RecordParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

#[derive(Debug)]
pub(crate) struct ReaderCreateError {
    pub(crate) text: String,
    pub(crate) source: Option<Box<dyn Error>>,
}

impl Display for ReaderCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. source {}",
            self.text,
            match &self.source {
                Some(err) => err.to_string(),
                None => "none".into(),
            }
        )
    }
}

impl Error for ReaderCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

use thiserror::Error;

#[derive(Error, Debug)]
#[error("write error: {text}")]
pub struct RecordWriteError {
    pub(crate) text: String,
    #[source]
    pub(crate) source: Option<Box<dyn Error>>,
}

// #[derive(Debug)]
// pub struct RecordWriteError {
//     pub(crate) text: String,
//     pub(crate) source: Option<Box<dyn Error>>,
// }

// impl Display for RecordWriteError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{}. source {}",
//             self.text,
//             match &self.source {
//                 Some(err) => err.to_string(),
//                 None => "none".into(),
//             }
//         )
//     }
// }

// impl Error for RecordWriteError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         self.source.as_deref()
//     }
// }
