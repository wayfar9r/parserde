use std::{
    array::TryFromSliceError,
    error::Error,
    io::{self, BufReader, Read, Write},
};

use crate::{
    error::RecordWriteError,
    record::{
        DataConsumer, DataProducer, Field, FieldValue, Record, RecordSerialize, RecordWriter,
        Status, TxType, fields,
    },
};

use crate::error::{FieldParseError, RecordParseError, RecordProduceError, RecordReadError};
use crate::result::{
    FieldParseResult, RecordParseResult, RecordProduceResult, RecordReadResult,
    RecordSerializeResult,
};

pub struct BinReader<T: Read> {
    reader: BufReader<T>,
    is_exhausted: bool,
}

impl<T: Read> BinReader<T> {
    pub fn new(reader: T) -> Result<BinReader<T>, Box<dyn Error>> {
        Ok(BinReader {
            reader: BufReader::new(reader),
            is_exhausted: false,
        })
    }
}

fn try_u32_from_bytes(b: &[u8]) -> Result<u32, TryFromSliceError> {
    Ok(u32::from_be_bytes(b.try_into()?))
}

impl<T: Read> DataConsumer for BinReader<T> {
    type Item = Vec<u8>;
    fn read(&mut self) -> Option<RecordReadResult<Self::Item>> {
        if self.is_exhausted {
            return None;
        }
        let mut head_buf = [0u8; 8];
        match self.reader.read_exact(&mut head_buf) {
            // change to if let err
            Ok(_) => (),
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => {
                    self.is_exhausted = true;
                    return None;
                }
                _ => {
                    return Some(Err(RecordReadError {
                        text: "failed to read head bytes".into(),
                        source: Some(Box::new(e)),
                    }));
                }
            },
        };

        let body_size = match try_u32_from_bytes(&head_buf[4..]) {
            Ok(b) => b,
            Err(e) => {
                return Some(Err(RecordReadError {
                    text: "failed to parse body size".into(),
                    source: Some(Box::new(e)),
                }));
            }
        };

        let mut body_buf = vec![0u8; body_size as usize];
        if let Err(e) = self.reader.read_exact(&mut body_buf) {
            match e.kind() {
                io::ErrorKind::UnexpectedEof => {
                    self.is_exhausted = true;
                    return None;
                }
                _ => {
                    return Some(Err(RecordReadError {
                        text: "failed to read boy".into(),
                        source: Some(Box::new(e)),
                    }));
                }
            }
        }
        Some(Ok(body_buf))
    }
}

impl<T: Read> DataProducer for BinReader<T> {
    fn produce_record(&mut self) -> Option<RecordProduceResult<Record>> {
        let result = match self.read() {
            Some(result) => match result {
                Ok(bytes) => bytes,
                Err(e) => {
                    return Some(Err(RecordProduceError {
                        text: "failed to read record".into(),
                        source: Some(Box::new(e)),
                    }));
                }
            },
            None => return None,
        };
        match parse_body(result) {
            Ok(r) => Some(Ok(r)),
            Err(e) => {
                return Some(Err(RecordProduceError {
                    text: "failed to parse record".into(),
                    source: Some(Box::new(e)),
                }));
            }
        }
    }
}

fn try_u64_from_bytes(bytes: &[u8]) -> Result<u64, TryFromSliceError> {
    Ok(u64::from_be_bytes(bytes.try_into()?))
}

impl Field<&str, &[u8]> {
    pub fn parse(&self) -> FieldParseResult<FieldValue> {
        Ok(match self.name {
            fields::str::TX_ID => {
                FieldValue::TxId(try_u64_from_bytes(self.value).map_err(|_| FieldParseError {
                    text: "failed to parse tx_id".into(),
                    source: None,
                })?)
            }
            fields::str::TX_TYPE => {
                FieldValue::TxType(TxType::try_from(&self.value[0]).map_err(|_| {
                    FieldParseError {
                        text: "failed to parse tx_type".into(),
                        source: None,
                    }
                })?)
            }
            fields::str::STATUS => {
                FieldValue::Status(Status::try_from(&self.value[0]).map_err(|_| {
                    FieldParseError {
                        text: "failed to parse status".into(),
                        source: None,
                    }
                })?)
            }
            fields::str::FROM_USER => {
                FieldValue::FromUser(try_u64_from_bytes(self.value).map_err(|_| {
                    FieldParseError {
                        text: "failed to parse from_user".into(),
                        source: None,
                    }
                })?)
            }
            fields::str::TO_USER => {
                FieldValue::ToUser(try_u64_from_bytes(self.value).map_err(|_| FieldParseError {
                    text: "failed to parse to_user".into(),
                    source: None,
                })?)
            }
            fields::str::AMOUNT => {
                FieldValue::Amount(try_u64_from_bytes(self.value).map_err(|_| FieldParseError {
                    text: "failed to parse amount".into(),
                    source: None,
                })?)
            }
            fields::str::TIMESTAMP => {
                FieldValue::Timestamp(try_u64_from_bytes(self.value).map_err(|_| {
                    FieldParseError {
                        text: format!("failed to parse timestamp"),
                        source: None,
                    }
                })?)
            }
            fields::str::DESCRIPTION => {
                FieldValue::Description(String::from_utf8(self.value.to_owned()).map_err(|e| {
                    FieldParseError {
                        text: "failed to parse description".into(),
                        source: Some(Box::new(e)),
                    }
                })?)
            }
            _ => {
                return Err(FieldParseError {
                    text: format!("unknown field: {}", self.name),
                    source: None,
                });
            }
        })
    }
}

fn parse_body(body: Vec<u8>) -> RecordParseResult<Record> {
    let fields_to_parse = [
        (fields::str::TX_ID, &body[0..8]),
        (fields::str::TX_TYPE, &body[8..9]),
        (fields::str::FROM_USER, &body[9..17]),
        (fields::str::TO_USER, &body[17..25]),
        (fields::str::AMOUNT, &body[25..33]),
        (fields::str::TIMESTAMP, &body[33..41]),
        (fields::str::STATUS, &body[41..42]),
        (fields::str::DESCRIPTION, &body[46..]),
    ];
    let mut fields: Vec<FieldValue> = Vec::with_capacity(8);
    for (n, b) in fields_to_parse {
        let f = Field::new(n, b).parse().map_err(|e| RecordParseError {
            text: format!("failed to parse field {}", n),
            source: Some(Box::new(e)),
        })?;
        fields.push(f);
    }
    Ok(Record::try_from(fields).map_err(|e| RecordParseError {
        text: "failed to parse record".into(),
        source: Some(e.into()),
    })?)
}

#[derive(Debug)]
pub struct RecordBytes;

impl RecordSerialize for RecordBytes {
    fn serialize(&self, record: &Record) -> RecordSerializeResult<Vec<u8>> {
        let mut r = Vec::from("YPBN");
        let desc_length = record.description.len() as u32;
        r.extend_from_slice(&(46 + desc_length).to_be_bytes());
        r.extend_from_slice(&record.tx_id.to_be_bytes());
        r.push(u8::from(&record.tx_type));
        r.extend_from_slice(&record.from_user.to_be_bytes());
        r.extend_from_slice(&record.to_user.to_be_bytes());
        r.extend_from_slice(&record.amount.to_be_bytes());
        r.extend_from_slice(&record.timestamp.to_be_bytes());
        r.push(u8::from(&record.status));
        r.extend_from_slice(&(desc_length).to_be_bytes());
        r.extend_from_slice(record.description.as_bytes());
        Ok(r)
    }
}

pub struct RecordWrite<W: Write> {
    writer: W,
}

impl<W: Write> RecordWrite<W> {
    pub fn new(writer: W) -> RecordWrite<W> {
        RecordWrite { writer }
    }
}

impl<W: Write> RecordWriter for RecordWrite<W> {
    fn write(&mut self, data: Vec<u8>) -> crate::result::RecordWriteResult<()> {
        match self.writer.write(&data) {
            Ok(_) => Ok(()),
            Err(e) => Err(RecordWriteError {
                text: "failed to write data".into(),
                source: Some(Box::new(e)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read() {
        let record1 = Record::new(
            1,
            TxType::DEPOSIT,
            2,
            3,
            1000,
            1000000000000,
            Status::SUCCESS,
            "Description 1".into(),
        );
        let record2 = Record::new(
            2,
            TxType::WITHDRAWAL,
            6,
            5,
            1456,
            1000009000001,
            Status::PENDING,
            "Description 2".into(),
        );
        let bin_ser = RecordBytes;
        let cursor = Cursor::new(
            [
                bin_ser.serialize(&record1).unwrap(),
                bin_ser.serialize(&record2).unwrap(),
            ]
            .concat(),
        );
        let mut bin_reader = BinReader::new(cursor).unwrap();
        let read_result = bin_reader.produce_record();
        assert!(read_result.is_some());
        let parse_result = read_result.unwrap();
        println!("{:?}", parse_result);
        assert!(parse_result.is_ok());
        let record1_parsed = parse_result.unwrap();
        assert_eq!(record1_parsed, record1);
        let record2_parsed = bin_reader.produce_record().unwrap().unwrap();
        assert_eq!(record2_parsed, record2);
        assert!(bin_reader.read().is_none());
    }
}
