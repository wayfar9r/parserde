use std::io::{self, BufRead, BufReader, Read, Write};

use crate::record::{
    Data, DataConsumer, DataProducer, FieldValue, Record, RecordSerialize, RecordWriter, fields,
};

use crate::error::{RecordProduceError, RecordReadError, RecordSerializeError, RecordWriteError};
use crate::result::{ReaderCreateResult, RecordProduceResult, RecordReadResult};

pub struct TxtReader<T: Read> {
    pub(crate) reader: BufReader<T>,
    current_line: u64,
    is_exhausted: bool,
}

impl<T: Read> TxtReader<T> {
    pub fn new(reader: T) -> ReaderCreateResult<TxtReader<T>> {
        Ok(TxtReader {
            reader: BufReader::new(reader),
            current_line: 0,
            is_exhausted: false,
        })
    }

    fn read_payload(&mut self) -> Option<Result<String, io::Error>> {
        let payload = loop {
            let mut buf = String::new();
            let bytes_read = match self.reader.read_line(&mut buf) {
                Ok(count) => count,
                Err(err) => return Some(Err(err)),
            };
            if bytes_read == 0 {
                self.is_exhausted = true;
                return None;
            }
            self.current_line += 1;
            if buf.ends_with('\n') {
                let _ = buf.pop();
            }
            if buf.starts_with('#') {
                continue;
            }
            break buf;
        };
        Some(Ok(payload))
    }
}

impl<T: Read> DataConsumer for TxtReader<T> {
    type Item = String;
    fn read(&mut self) -> Option<RecordReadResult<Self::Item>> {
        if self.is_exhausted {
            return None;
        }
        let read_result = self.read_payload()?;
        match read_result {
            Ok(r) => Some(Ok(r)),
            Err(e) => {
                return Some(Err(RecordReadError {
                    text: "couldn't read data".to_string(),
                    source: Some(Box::new(e)),
                }));
            }
        }
    }
}

impl<T: Read> DataProducer for TxtReader<T> {
    fn produce_record(&mut self) -> Option<RecordProduceResult<Record>> {
        let mut fields = Vec::new();
        while let Some(read_result) = self.read() {
            if let Err(e) = read_result {
                return Some(Err(RecordProduceError {
                    text: format!("failed to read line {}", self.current_line),
                    source: Some(Box::new(e)),
                }));
            }
            let line = read_result.unwrap();
            if line.is_empty() {
                break;
            }
            match FieldValue::try_from(Data::new(line)) {
                Ok(val) => fields.push(val),
                Err(e) => {
                    return Some(Err(RecordProduceError {
                        text: format!("failed to parse field. line {}", self.current_line),
                        source: Some(Box::new(e)),
                    }));
                }
            }
        }
        if fields.len() == 0 {
            return None;
        }
        match Record::try_from(fields) {
            Ok(r) => Some(Ok(r)),
            Err(e) => Some(Err(RecordProduceError {
                text: format!("failed to parse record. near line {}", self.current_line),
                source: Some(e.into()),
            })),
        }
    }
}

pub struct TxtSerialize;

impl RecordSerialize for TxtSerialize {
    fn serialize(&self, record: &Record) -> Result<Vec<u8>, RecordSerializeError> {
        let fields = [
            format!("{}: {}", fields::str::TX_ID, &record.tx_id),
            format!("{}: {}", fields::str::AMOUNT, &record.amount),
            format!("{}: {}", fields::str::TIMESTAMP, &record.timestamp),
            format!("{}: {}", fields::str::DESCRIPTION, &record.description),
            format!("{}: {}", fields::str::TX_TYPE, &record.tx_type),
            format!("{}: {}", fields::str::FROM_USER, &record.from_user),
            format!("{}: {}", fields::str::TO_USER, &record.to_user),
            format!("{}: {}\n", fields::str::STATUS, &record.status),
        ];
        Ok(fields.join("\n").into_bytes())
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
    fn write(&mut self, mut data: Vec<u8>) -> crate::result::RecordWriteResult<()> {
        data.push('\n' as u8);
        // data.push('\n' as u8);
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
mod txt_tests {

    use std::io::Cursor;

    struct FieldLineIterator<T: Read> {
        inner: TxtReader<T>,
    }

    #[derive(Debug, PartialEq)]
    enum FieldOrEmpty {
        Field(String),
        Empty,
    }

    impl<T: Read> Iterator for FieldLineIterator<T> {
        type Item = FieldOrEmpty;
        fn next(&mut self) -> Option<Self::Item> {
            match self.inner.read() {
                Some(r) => match r {
                    Ok(s) => (!s.is_empty())
                        .then_some(FieldOrEmpty::Field(s))
                        .or(Some(FieldOrEmpty::Empty)),

                    Err(_) => None,
                },
                None => None,
            }
        }
    }

    impl<T: Read> TxtReader<T> {
        fn fields(self) -> FieldLineIterator<T> {
            FieldLineIterator { inner: self }
        }
    }

    use super::*;

    fn get_good_input() -> String {
        "# Record 2 (TRANSFER)\nDESCRIPTION: \"Record number 2\"
TIMESTAMP: 1633036920000
STATUS: PENDING
AMOUNT: 200
TX_ID: 1000000000000001
TX_TYPE: TRANSFER
FROM_USER_ID: 9223372036854775807
TO_USER_ID: 9223372036854775807

# Record 7 (DEPOSIT)
TO_USER_ID: 728970204360217851
TX_TYPE: DEPOSIT
AMOUNT: 700
DESCRIPTION: \"Record number 7\"
STATUS: FAILURE
FROM_USER_ID: 0
TIMESTAMP: 1633037220000
TX_ID: 1000000000000006"
            .to_string()
    }

    #[test]
    fn test_data_read() {
        let cursor = Cursor::new(get_good_input());
        let mut data_consumer = TxtReader::new(cursor).unwrap().fields();
        assert_eq!(
            data_consumer.next().unwrap(),
            FieldOrEmpty::Field("DESCRIPTION: \"Record number 2\"".to_string())
        );
        assert_eq!(data_consumer.inner.current_line, 2);
        assert_eq!(
            data_consumer.nth(6).unwrap(),
            FieldOrEmpty::Field("TO_USER_ID: 9223372036854775807".into())
        );
        assert_eq!(data_consumer.inner.current_line, 9);
        assert_eq!(data_consumer.next().unwrap(), FieldOrEmpty::Empty);
        // let result = data_consumer.read();
        assert_eq!(
            data_consumer.next().unwrap(),
            FieldOrEmpty::Field("TO_USER_ID: 728970204360217851".into())
        );
        assert_eq!(
            data_consumer.nth(6).unwrap(),
            FieldOrEmpty::Field("TX_ID: 1000000000000006".into())
        );
        assert!(data_consumer.next().is_none());
        assert_eq!(data_consumer.inner.current_line, 19);
    }

    #[test]
    fn test_record_produce() {
        let input = get_good_input();
        let c = Cursor::new(input);
        let mut tx = TxtReader::new(c).unwrap();
        let record = tx.produce_record();
        assert!(record.is_some());
        let record = record.unwrap().unwrap();
        assert_eq!(record.tx_id, 1000000000000001);
        let record2 = tx.produce_record().unwrap().unwrap();
        assert_eq!(record2.tx_id, 1000000000000006);
        assert!(tx.produce_record().is_none());
    }
}
