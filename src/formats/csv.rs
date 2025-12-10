use std::{
    error::Error,
    io::{BufRead, BufReader, Read, Write},
};

use crate::{
    error::RecordWriteError,
    record::{DataConsumer, DataProducer, Field, Record, RecordSerialize, RecordWriter, fields},
};

use crate::error::{RecordProduceError, RecordReadError, RecordSerializeError};
use crate::result::{RecordProduceResult, RecordReadResult, RecordSerializeResult};

pub struct CsvReader<T: Read> {
    pub(crate) reader: BufReader<T>,
    pub(crate) order: Vec<String>,
    pub(crate) separator: String,
    pub(crate) current_line: u64,
    is_exhausted: bool,
}

impl<T: Read> CsvReader<T> {
    pub fn new(reader: T, separator: &str) -> Result<CsvReader<T>, Box<dyn Error>> {
        let mut buf_reader = BufReader::new(reader);
        let fields = read_header(&mut buf_reader, separator)?;
        Ok(CsvReader {
            reader: buf_reader,
            order: fields,
            separator: separator.to_owned(),
            current_line: 0,
            is_exhausted: false,
        })
    }
}

fn read_header<T: Read>(
    reader: &mut BufReader<T>,
    separator: &str,
) -> Result<Vec<String>, RecordReadError> {
    let mut buf = String::new();
    reader.read_line(&mut buf).map_err(|err| RecordReadError {
        text: "couldn't read header".into(),
        source: Some(Box::new(err)),
    })?;
    buf.pop();
    Ok(buf
        .split(separator)
        .map(|s| s.to_owned())
        .collect::<Vec<String>>())
}

impl<T: Read> DataConsumer for CsvReader<T> {
    type Item = String;
    fn read(&mut self) -> Option<RecordReadResult<Self::Item>> {
        if self.is_exhausted {
            return None;
        }
        let mut buf = String::new();
        let bytes_read = match self.reader.read_line(&mut buf) {
            Ok(read) => read,
            Err(err) => {
                return Some(Err(RecordReadError {
                    text: format!("failed to read line {}", self.current_line),
                    source: Some(Box::new(err)),
                }));
            }
        };
        if bytes_read == 0 {
            self.is_exhausted = true;
            return None;
        }
        self.current_line += 1;
        if buf.ends_with('\n') {
            let _ = buf.pop();
        }
        Some(Ok(buf))
    }
}

impl<T: Read> DataProducer for CsvReader<T> {
    fn produce_record(&mut self) -> Option<RecordProduceResult<Record>> {
        let read_result = self.read()?;
        let payload = match read_result {
            Ok(data) => data,
            Err(e) => {
                return Some(Err(RecordProduceError {
                    text: format!("failed to produce record. line {}", self.current_line),
                    source: Some(Box::new(e)),
                }));
            }
        };
        let mut fields = Vec::new();
        let mut value_iter = payload.split(&self.separator);
        for f in self.order.iter() {
            match value_iter.next() {
                Some(val) => match Field::new(f.as_str(), val).parse() {
                    Ok(val) => fields.push(val),
                    Err(e) => {
                        return Some(Err(RecordProduceError {
                            text: format!("failed to produce record. line {}", self.current_line),
                            source: Some(Box::new(e)),
                        }));
                    }
                },
                None => {
                    return Some(Err(RecordProduceError {
                        text: format!("missing field {}. near line {}", f, self.current_line),
                        source: None,
                    }));
                }
            };
        }
        match Record::try_from(fields) {
            Ok(r) => Some(Ok(r)),
            Err(e) => {
                return Some(Err(RecordProduceError {
                    text: format!("couldn't parse record. near line {}", self.current_line),
                    source: Some(e.into()),
                }));
            }
        }
    }
}

pub struct CsvSerialize<'a> {
    fields: &'a [&'a str],
    separator: &'a str,
}

impl<'a> CsvSerialize<'a> {
    pub fn new(fields: &'a [&'a str], separator: &'a str) -> CsvSerialize<'a> {
        CsvSerialize { fields, separator }
    }
}

impl<'a> RecordSerialize for CsvSerialize<'a> {
    fn serialize(&self, record: &Record) -> RecordSerializeResult<Vec<u8>> {
        let mut r = Vec::new();
        for &f in self.fields {
            match f {
                fields::str::TX_ID => {
                    r.push(record.tx_id.to_string());
                }
                fields::str::TX_TYPE => {
                    r.push(record.tx_type.to_string());
                }
                fields::str::FROM_USER => {
                    r.push(record.from_user.to_string());
                }
                fields::str::TO_USER => {
                    r.push(record.to_user.to_string());
                }
                fields::str::AMOUNT => {
                    r.push(record.amount.to_string());
                }
                fields::str::TIMESTAMP => {
                    r.push(record.timestamp.to_string());
                }
                fields::str::DESCRIPTION => {
                    r.push(record.description.to_owned());
                }
                fields::str::STATUS => {
                    r.push(record.status.to_string());
                }
                _ => {
                    return Err(RecordSerializeError {
                        text: format!("unknown field {}", f),
                        source: None,
                    });
                }
            }
        }
        Ok(r.join(self.separator).into_bytes())
    }
}

pub struct RecordWrite<'a, W: Write> {
    fields: &'a [&'a str],
    separator: String,
    writer: W,
}

impl<'a, W: Write> RecordWrite<'a, W> {
    pub fn new(writer: W, fields: &'a [&'a str], separator: String) -> RecordWrite<'a, W> {
        RecordWrite {
            fields,
            writer,
            separator,
        }
    }
}

impl<'a, W: Write> RecordWriter for RecordWrite<'a, W> {
    fn write_header(&mut self) -> crate::result::RecordWriteResult<()> {
        let mut header = self.fields.join(&self.separator).into_bytes();
        header.push('\n' as u8);
        match self.writer.write(&header) {
            Ok(_) => Ok(()),
            Err(e) => Err(RecordWriteError {
                text: "failed to write csv header".into(),
                source: Some(Box::new(e)),
            }),
        }
    }
    fn write(&mut self, mut data: Vec<u8>) -> crate::result::RecordWriteResult<()> {
        data.push('\n' as u8);
        match self.writer.write(&data) {
            Ok(_) => Ok(()),
            Err(e) => Err(RecordWriteError {
                text: "failed to write record".into(),
                source: Some(Box::new(e)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn get_good_input() -> String {
        "TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION
1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1
1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2".to_string()
    }

    #[test]
    fn test_read() {
        let input = Cursor::new(get_good_input());
        let mut reader = CsvReader::new(input, ",").unwrap();
        let result = reader.read();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(
            &result,
            "1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,\"Record number 1"
        );
        let result = reader.read();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(
            &result,
            "1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,\"Record number 2"
        );
        assert!(reader.read().is_none());
    }

    #[test]
    fn test_produce_record() {
        let input = Cursor::new(get_good_input());
        let mut reader = CsvReader::new(input, ",").unwrap();
        let result = reader.produce_record();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(result.tx_id, 1000000000000000);
        let result = reader.produce_record().unwrap().unwrap();
        assert_eq!(result.tx_id, 1000000000000001);
        assert!(reader.read().is_none());
    }
}
