use std::fmt;

use csv;

use crate::csvql::query::frame;
use crate::csvql::query::error;

// A data selector
pub trait Selector {
  fn select(&self, schema: &frame::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error>;
}

impl<S: Selector + ?Sized> Selector for Box<S> { // black magic
  fn select(&self, schema: &frame::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
    (**self).select(schema, row)
  }
}

// macro_rules! join {
//   () => (
//     vec::Vec::new()
//   );
//   ($elem:expr; $n:expr) => (
//     vec::from_elem($elem, $n)
//   );
//   ($($x:expr),+ $(,)?) => (
//     Join::new(<[_]>::into_vec(
//       Box::new([$($x),+])
//     ))
//   );
// }

// pub struct Join {
//   sels: Vec<Box<dyn Selector>>
// }

// impl Join {
//   pub fn new(sels: Vec<Box<dyn Selector>>) -> Join {
//     Join{
//       sels: sels,
//     }
//   }
  
//   pub fn new_with_columns(sels: Vec<Column>) -> Join {
//     let mut cast: Vec<Box<dyn Selector>> = Vec::new();
//     for sel in sels {
//       cast.push(Box::new(sel));
//     }
//     Join{
//       sels: cast,
//     }
//   }
// }

// impl Selector for Join {
//   fn select(&self, schema: &frame::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
//     if self.sels.len() == 0 {
//       return Ok(row.to_owned());
//     }
//     let mut res: Vec<String> = Vec::new();
//     for sel in &self.sels {
//       res.append(&mut sel.select(schema, row)?.iter().map(|e| { e.to_owned() }).collect());
//     }
//     Ok(res.into())
//   }
// }

// impl fmt::Display for Join {
//   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//     write!(f, "join {}", self.sels.len())
//   }
// }

// #[derive(Debug, Clone)]
// pub struct Column {
//   from: Option<String>,
//   name: String,
//   index: usize,
// }

// impl Column {
//   pub fn parse(text: &str) -> Result<Column, error::Error> {
//     let parts: Vec<&str> = text.rsplitn(2, ".").collect();
//     match parts.len() {
//       0 => Err(error::ParseError::new("Empty selector").into()),
//       1 => Ok(Self::new(None, parts[0], 0)),
//       2 => Ok(Self::new(Some(parts[0]), parts[1], 0)),
//       _ => Err(error::ParseError::new(&format!("Invalid selector: {}", text)).into()),
//     }
//   }
  
//   pub fn new(from: Option<&str>, name: &str, index: usize) -> Column {
//     let from = match from {
//       Some(from) => Some(from.to_owned()),
//       None => None,
//     };
//     Column{
//       from: from,
//       name: name.to_owned(),
//       index: index,
//     }
//   }
  
//   pub fn from<'a>(&'a self) -> Option<&'a str> {
//     match &self.from {
//       Some(from) => Some(from),
//       None => None,
//     }
//   }
  
//   pub fn name<'a>(&'a self) -> &'a str {
//     &self.name
//   }
  
//   pub fn index<'a>(&'a self) -> usize {
//     self.index
//   }
// }

// impl Selector for Column {
//   fn select(&self, schema: &frame::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
//     let mut sel: Vec<String> = Vec::new();
//     if let Some(index) = schema.index(&self.name) {
//       if let Some(col) = row.get(index) {
//         sel.push(col.to_owned());
//       }
//     }
//     Ok(sel.into())
//   }
// }

// impl fmt::Display for Column {
//   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//     write!(f, "column: {}", self.name)
//   }
// }
