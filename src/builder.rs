/// sdfsdf
use crate::bin::{BinReader, RecordBytes, RecordWrite as BinWrite};
use crate::csv::{CsvReader, CsvSerialize, RecordWrite as CsvWrite};
use crate::record::{DataProducer, RecordSerialize, RecordWriter, fields};
use crate::txt::{RecordWrite as TxtWrite, TxtReader, TxtSerialize};
use std::error::Error;
use std::io::{Read, Write};

///Creates an appropriate reader depending on input format
///
/// ```
/// use parserde::build_reader;
/// let cursor = std::io::Cursor::new("hello");
/// let reader = build_reader(cursor, "txt");
/// ```
///
pub fn build_reader<T: Read + 'static>(
    reader: T,
    format: &str,
) -> Result<Box<dyn DataProducer>, Box<dyn Error>> {
    Ok(match format {
        "csv" => Box::new(CsvReader::new(reader, b',')?),
        "txt" => Box::new(TxtReader::new(reader)?),
        "bin" => Box::new(BinReader::new(reader)?),
        _ => return Err(format!("given an unsupported format {}", format).into()),
    })
}

/// Creates a serializer depending on input format
///
/// ```
/// use parserde::build_serializer;
/// let serializer = build_serializer("bin");
/// ```
///
pub fn build_serializer(format: &str) -> Result<Box<dyn RecordSerialize>, Box<dyn Error>> {
    Ok(match format {
        "csv" => Box::new(CsvSerialize::new(
            &[
                fields::str::TX_ID,
                fields::str::TX_TYPE,
                fields::str::FROM_USER,
                fields::str::TO_USER,
                fields::str::AMOUNT,
                fields::str::TIMESTAMP,
                fields::str::STATUS,
                fields::str::DESCRIPTION,
            ],
            ",",
        )),
        "bin" => Box::new(RecordBytes),
        "txt" => Box::new(TxtSerialize),
        _ => return Err(format!("given an unsupported format {}", format).into()),
    })
}

/// Creates a writer depending on input format
///
/// ```
/// use parserde::build_writer;
/// let buf: Vec<u8> = Vec::new();
/// let writer = build_writer(buf, "csv");
/// ```
///
pub fn build_writer<W: Write + 'static>(
    writer: W,
    output_format: &str,
) -> Result<Box<dyn RecordWriter>, Box<dyn Error>> {
    Ok(match output_format {
        "csv" => Box::new(CsvWrite::new(
            writer,
            &[
                fields::str::TX_ID,
                fields::str::TX_TYPE,
                fields::str::FROM_USER,
                fields::str::TO_USER,
                fields::str::AMOUNT,
                fields::str::TIMESTAMP,
                fields::str::STATUS,
                fields::str::DESCRIPTION,
            ],
            ",".into(),
        )),
        "txt" => Box::new(TxtWrite::new(writer)),
        "bin" => Box::new(BinWrite::new(writer)),
        _ => return Err(format!("given an unsupported format {}", output_format).into()),
    })
}
