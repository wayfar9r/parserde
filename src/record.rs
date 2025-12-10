use std::fmt::Display;

use crate::error::FieldParseError;
use crate::result::{
    FieldParseResult, RecordProduceResult, RecordReadResult, RecordSerializeResult,
    RecordWriteResult,
};

#[derive(Debug)]
pub struct Field<F, V> {
    pub(crate) name: F,
    pub(crate) value: V,
}

impl Field<&str, &str> {
    pub fn parse(&self) -> FieldParseResult<FieldValue> {
        Ok(match self.name.as_bytes() {
            fields::byte::TX_ID => {
                FieldValue::TxId(self.value.parse().map_err(|err| FieldParseError {
                    text: "failed to parse tx_id".into(),
                    source: Some(Box::new(err)),
                })?)
            }
            fields::byte::TX_TYPE => {
                FieldValue::TxType(TxType::try_from(&self.value[..]).map_err(|err| {
                    FieldParseError {
                        text: "failed to parse tx_type".into(),
                        source: Some(err.into()),
                    }
                })?)
            }
            fields::byte::STATUS => {
                FieldValue::Status(Status::try_from(&self.value[..]).map_err(|err| {
                    FieldParseError {
                        text: "failed to parse status".into(),
                        source: Some(err.into()),
                    }
                })?)
            }
            fields::byte::FROM_USER => {
                FieldValue::FromUser(self.value.parse().map_err(|err| FieldParseError {
                    text: "failed to parse from_user".into(),
                    source: Some(Box::new(err)),
                })?)
            }
            fields::byte::TO_USER => {
                FieldValue::ToUser(self.value.parse().map_err(|err| FieldParseError {
                    text: "failed to parse to_user".into(),
                    source: Some(Box::new(err)),
                })?)
            }
            fields::byte::AMOUNT => {
                FieldValue::Amount(self.value.parse().map_err(|err| FieldParseError {
                    text: "failed to parse amount".into(),
                    source: Some(Box::new(err)),
                })?)
            }
            fields::byte::TIMESTAMP => {
                FieldValue::Timestamp(self.value.parse().map_err(|err| FieldParseError {
                    text: format!("failed to parse timestamp"),
                    source: Some(Box::new(err)),
                })?)
            }
            fields::byte::DESCRIPTION => FieldValue::Description(self.value.to_owned()),
            _ => {
                return Err(FieldParseError {
                    text: format!("unknown field: {}", self.name),
                    source: None,
                });
            }
        })
    }
}

impl<F, V> Field<F, V> {
    pub fn new(name: F, value: V) -> Field<F, V> {
        Field { name, value }
    }
}

impl<F: Display, V: Display> Display for Field<F, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.value)
    }
}

pub trait DataConsumer {
    type Item;
    fn read(&mut self) -> Option<RecordReadResult<Self::Item>>;
}

/// Trait for types that return parsed Record structure
pub trait DataProducer {
    fn produce_record(&mut self) -> Option<RecordProduceResult<Record>>;
}

pub mod fields {
    pub mod str {
        pub const TX_ID: &str = "TX_ID";
        pub const TX_TYPE: &str = "TX_TYPE";
        pub const STATUS: &str = "STATUS";
        pub const FROM_USER: &str = "FROM_USER_ID";
        pub const TO_USER: &str = "TO_USER_ID";
        pub const TIMESTAMP: &str = "TIMESTAMP";
        pub const AMOUNT: &str = "AMOUNT";
        pub const DESCRIPTION: &str = "DESCRIPTION";
    }
    pub mod byte {
        pub const TX_ID: &[u8] = b"TX_ID";
        pub const TX_TYPE: &[u8] = b"TX_TYPE";
        pub const STATUS: &[u8] = b"STATUS";
        pub const FROM_USER: &[u8] = b"FROM_USER_ID";
        pub const TO_USER: &[u8] = b"TO_USER_ID";
        pub const TIMESTAMP: &[u8] = b"TIMESTAMP";
        pub const AMOUNT: &[u8] = b"AMOUNT";
        pub const DESCRIPTION: &[u8] = b"DESCRIPTION";
    }
}

pub enum FieldValue {
    TxId(u64),
    TxType(TxType),
    Status(Status),
    FromUser(u64),
    ToUser(u64),
    Timestamp(u64),
    Amount(u64),
    Description(String),
}

#[derive(Debug, PartialEq)]
pub enum TxType {
    DEPOSIT,
    TRANSFER,
    WITHDRAWAL,
}

impl Display for TxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TxType::DEPOSIT => "DEPOSIT",
                TxType::TRANSFER => "TRANSFER",
                TxType::WITHDRAWAL => "WITHDRAWAL",
            }
        )
    }
}

#[derive(Debug, PartialEq)]
pub enum Status {
    SUCCESS,
    FAILURE,
    PENDING,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Status::FAILURE => "FAILURE",
                Status::PENDING => "PENDING",
                Status::SUCCESS => "SUCCESS",
            }
        )
    }
}

impl TryFrom<&u8> for TxType {
    type Error = String;
    fn try_from(value: &u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => TxType::DEPOSIT,
            1 => TxType::TRANSFER,
            2 => TxType::WITHDRAWAL,
            _ => return Err("couldn't convert byte to tx_type".to_string()),
        })
    }
}

impl From<&TxType> for u8 {
    fn from(value: &TxType) -> Self {
        match value {
            TxType::DEPOSIT => 0,
            TxType::TRANSFER => 1,
            TxType::WITHDRAWAL => 2,
        }
    }
}

impl TryFrom<&str> for TxType {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "DEPOSIT" => TxType::DEPOSIT,
            "TRANSFER" => TxType::TRANSFER,
            "WITHDRAWAL" => TxType::WITHDRAWAL,
            _ => return Err("invalid TxType".to_string()),
        })
    }
}

impl From<TxType> for &str {
    fn from(value: TxType) -> Self {
        match value {
            TxType::DEPOSIT => "DEPOSIT",
            TxType::WITHDRAWAL => "WITHDRAWAL",
            TxType::TRANSFER => "TRANSFER",
        }
    }
}

impl From<&Status> for u8 {
    fn from(value: &Status) -> Self {
        match value {
            Status::SUCCESS => 0,
            Status::FAILURE => 1,
            Status::PENDING => 2,
        }
    }
}

impl TryFrom<&str> for Status {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "SUCCESS" => Status::SUCCESS,
            "FAILURE" => Status::FAILURE,
            "PENDING" => Status::PENDING,
            _ => return Err("unknown status".to_string()),
        })
    }
}

impl From<Status> for &str {
    fn from(value: Status) -> Self {
        match value {
            Status::FAILURE => "FAILURE",
            Status::SUCCESS => "STATUS",
            Status::PENDING => "PENDING",
        }
    }
}

impl TryFrom<&u8> for Status {
    type Error = String;
    fn try_from(value: &u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Status::SUCCESS,
            1 => Status::FAILURE,
            2 => Status::PENDING,
            _ => return Err("couldn't convert status to byte".to_string()),
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Record {
    pub(crate) tx_id: u64,
    pub(crate) tx_type: TxType,
    pub(crate) from_user: u64,
    pub(crate) to_user: u64,
    pub(crate) amount: u64,
    pub(crate) timestamp: u64,
    pub(crate) status: Status,
    pub(crate) description: String,
}

impl Record {
    pub fn new(
        tx_id: u64,
        tx_type: TxType,
        from_user: u64,
        to_user: u64,
        amount: u64,
        timestamp: u64,
        status: Status,
        description: String,
    ) -> Record {
        Record {
            tx_id,
            tx_type,
            from_user,
            to_user,
            amount,
            status,
            description,
            timestamp,
        }
    }
}

impl Default for Record {
    fn default() -> Self {
        Record {
            tx_id: u64::default(),
            tx_type: TxType::DEPOSIT,
            from_user: u64::default(),
            to_user: u64::default(),
            amount: u64::default(),
            timestamp: u64::default(),
            status: Status::PENDING,
            description: "".to_string(),
        }
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ {}, {}, {}, {}, {}, {}, {}, {} }}",
            self.tx_id,
            self.tx_type,
            self.status,
            self.from_user,
            self.to_user,
            self.timestamp,
            self.amount,
            self.description,
        )
    }
}

pub struct Data<T>(T);

impl TryFrom<Data<String>> for FieldValue {
    type Error = FieldParseError;
    fn try_from(mut field: Data<String>) -> FieldParseResult<FieldValue> {
        let del_pos = field.0.find(':');
        if let None = del_pos {
            return Err(FieldParseError {
                text: "no delimiter found".into(),
                source: None,
            });
        }
        let del_pos = del_pos.unwrap();
        let value = field.0.split_off(del_pos);
        let f = Field::new(field.0.as_str(), &value[2..]);
        f.parse()
    }
}

impl<T> Data<T> {
    pub fn new(value: T) -> Data<T> {
        Data(value)
    }
}

impl TryFrom<Vec<FieldValue>> for Record {
    type Error = String;
    fn try_from(value: Vec<FieldValue>) -> Result<Self, Self::Error> {
        let mut tx_id = None;
        let mut tx_type = None;
        let mut from_user = None;
        let mut to_user = None;
        let mut status = None;
        let mut amount = None;
        let mut timestamp = None;
        let mut description = None;
        for fval in value {
            match fval {
                FieldValue::TxId(val) => tx_id = Some(val),
                FieldValue::TxType(val) => tx_type = Some(val),
                FieldValue::Amount(val) => amount = Some(val),
                FieldValue::FromUser(val) => from_user = Some(val),
                FieldValue::ToUser(val) => to_user = Some(val),
                FieldValue::Timestamp(val) => timestamp = Some(val),
                FieldValue::Description(val) => description = Some(val),
                FieldValue::Status(val) => status = Some(val),
            }
        }
        Ok(Record {
            tx_id: tx_id.ok_or("missing field tx_id")?,
            tx_type: tx_type.ok_or("missing field tx_type")?,
            from_user: from_user.ok_or("missing field from_user")?,
            to_user: to_user.ok_or("missing field to_user")?,
            amount: amount.ok_or("missing field amount")?,
            timestamp: timestamp.ok_or("missing field timestamp")?,
            status: status.ok_or("missing field status")?,
            description: description.ok_or("missing field description")?,
        })
    }
}

pub trait RecordSerialize {
    fn serialize(&self, record: &Record) -> RecordSerializeResult<Vec<u8>>;
}

pub trait RecordWriter {
    fn write_header(&mut self) -> RecordWriteResult<()> {
        Ok(())
    }
    fn write(&mut self, data: Vec<u8>) -> RecordWriteResult<()>;
}
