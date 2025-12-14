use std::{
    error::Error,
    io::{Read, Write},
};

use csv::{Reader, ReaderBuilder, StringRecord};

use crate::{
    error::RecordWriteError,
    record::{DataConsumer, DataProducer, Field, Record, RecordSerialize, RecordWriter, fields},
};

use crate::error::{RecordProduceError, RecordReadError, RecordSerializeError};
use crate::result::{RecordProduceResult, RecordReadResult, RecordSerializeResult};

pub(crate) struct CsvReader<T: Read> {
    pub(crate) reader: Reader<T>,
    pub(crate) current_line: u64,
    is_exhausted: bool,
}

impl<T: Read> CsvReader<T> {
    pub(crate) fn new(reader: T, separator: u8) -> Result<CsvReader<T>, Box<dyn Error>> {
        let reader = ReaderBuilder::new()
            .delimiter(separator)
            .from_reader(reader);
        Ok(CsvReader {
            reader,
            current_line: 0,
            is_exhausted: false,
        })
    }
}

impl<T: Read> DataConsumer for CsvReader<T> {
    type Item = StringRecord;
    fn read(&mut self) -> Option<RecordReadResult<Self::Item>> {
        if self.is_exhausted {
            return None;
        }
        let mut string_record = StringRecord::new();
        match self.reader.read_record(&mut string_record) {
            Ok(not_done) => {
                if !not_done {
                    self.is_exhausted = true;
                    return None;
                }
            }
            Err(err) => {
                return Some(Err(RecordReadError {
                    text: format!("failed to read line {}", self.current_line),
                    source: Some(Box::new(err)),
                }));
            }
        };
        self.current_line += 1;
        Some(Ok(string_record))
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
        let mut value_iter = payload.iter();
        let header = match self.reader.headers().map_err(|e| RecordProduceError {
            text: "failed to read headers from csv file".into(),
            source: Some(Box::new(e)),
        }) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        for f in header {
            match value_iter.next() {
                Some(val) => match Field::new(f, val).parse() {
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
            Err(e) => Some(Err(RecordProduceError {
                text: format!("couldn't parse record. near line {}", self.current_line),
                source: Some(e.into()),
            })),
        }
    }
}

pub(crate) struct CsvSerialize<'a> {
    fields: &'a [&'a str],
    separator: &'a str,
}

impl<'a> CsvSerialize<'a> {
    pub(crate) fn new(fields: &'a [&'a str], separator: &'a str) -> CsvSerialize<'a> {
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

pub(crate) struct RecordWrite<'a, W: Write> {
    fields: &'a [&'a str],
    separator: String,
    writer: W,
}

impl<'a, W: Write> RecordWrite<'a, W> {
    pub(crate) fn new(writer: W, fields: &'a [&'a str], separator: String) -> RecordWrite<'a, W> {
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
        header.push(b'\n');
        match self.writer.write_all(&header) {
            Ok(_) => (),
            Err(e) => {
                return Err(RecordWriteError {
                    text: "failed to write csv header".into(),
                    source: Some(Box::new(e)),
                });
            }
        };
        match self.writer.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(RecordWriteError {
                text: "failed to write data".into(),
                source: Some(Box::new(e)),
            }),
        }
    }
    fn write(&mut self, mut data: Vec<u8>) -> crate::result::RecordWriteResult<()> {
        data.push(b'\n');
        match self.writer.write_all(&data) {
            Ok(_) => (),
            Err(e) => {
                return Err(RecordWriteError {
                    text: "failed to write data".into(),
                    source: Some(Box::new(e)),
                });
            }
        };
        match self.writer.flush() {
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

    fn get_good_input() -> String {
        "\
TX_ID,TX_TYPE,FROM_USER_ID,TO_USER_ID,AMOUNT,TIMESTAMP,STATUS,DESCRIPTION
1000000000000000,DEPOSIT,0,9223372036854775807,100,1633036860000,FAILURE,Record number 1
1000000000000001,TRANSFER,9223372036854775807,9223372036854775807,200,1633036920000,PENDING,Record number 2".to_string()
    }

    #[test]
    fn test_csv_lib() {
        let data = "\
TX_ID,TX_TYPE,FROM_USER_ID
Boston,United States,4628910
hewston,Australia,2
        ";
        let mut rdr = Reader::from_reader(Cursor::new(data.as_bytes()));
        let mut record = StringRecord::new();

        if rdr.read_record(&mut record).is_ok() {
            assert_eq!(record.as_slice(), "BostonUnited States4628910");
        } else {
            panic!("expected at least one record but got none")
        }
    }

    #[test]
    fn test_read() {
        let input = Cursor::new(get_good_input().into_bytes());
        let mut reader = CsvReader::new(input, b',').unwrap();
        let result = reader.read();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(
            result,
            vec![
                "1000000000000000",
                "DEPOSIT",
                "0",
                "9223372036854775807",
                "100",
                "1633036860000",
                "FAILURE",
                "Record number 1"
            ],
        );
        let result = reader.read();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(
            result,
            vec![
                "1000000000000001",
                "TRANSFER",
                "9223372036854775807",
                "9223372036854775807",
                "200",
                "1633036920000",
                "PENDING",
                "Record number 2"
            ]
        );
        let r = reader.read();
        println!("{:?}", r);
        assert!(r.is_none());
    }

    #[test]
    fn test_produce_record() {
        let input = Cursor::new(get_good_input());
        let mut reader = CsvReader::new(input, b',').unwrap();
        let result = reader.produce_record();
        assert!(result.is_some());
        let result = result.unwrap().unwrap();
        assert_eq!(result.tx_id, 1000000000000000);
        let result = reader.produce_record().unwrap().unwrap();
        assert_eq!(result.tx_id, 1000000000000001);
        assert!(reader.read().is_none());
    }
}
