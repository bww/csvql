use std::fmt;

use csv;

use crate::csvql::query::schema;
use crate::csvql::query::error;

// A data selector
pub trait Selector: fmt::Display + fmt::Debug {
  fn select(&self, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error>;
}

impl<S: Selector + ?Sized> Selector for Box<S> { // black magic
  fn select(&self, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
    (**self).select(row)
  }
}

#[derive(Clone)]
pub struct Columns {
  names: Vec<String>,
  indexes: Vec<usize>,
}

impl Columns {
  pub fn parse(schema: &schema::Schema, text: &Vec<String>) -> Result<Columns, error::Error> {
    let mut qnames: Vec<schema::QName> = Vec::new();
    for t in text {
      for e in t.split(",") {
        qnames.push(schema::QName::parse(e)?);
      }
    }
    Self::new(schema, &qnames)
  }
  
  pub fn new(schema: &schema::Schema, qnames: &Vec<schema::QName>) -> Result<Columns, error::Error> {
    let mut names: Vec<String> = Vec::new();
    let mut indexes: Vec<usize> = Vec::new();
    for qname in qnames {
      names.push(qname.to_string());
      indexes.push(match schema.index(qname) {
        Some(index) => index,
        None => return Err(error::QueryError::new(&format!("Index column not found: {} ({})", qname, schema)).into()),
      });
    }
    Ok(Columns{
      names: names,
      indexes: indexes,
    })
  }
}

impl Selector for Columns {
  fn select(&self, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
    let mut sel: Vec<String> = Vec::new();
    for index in &self.indexes {
      sel.push(match row.get(*index) {
        Some(col) => col.to_string(), // can we avoid this copy?
        None => return Err(error::QueryError::new(&format!("Index not found in data: {} > {}", index, row.len()-1)).into()),
      });
    }
    Ok(sel.into())
  }
}

impl fmt::Display for Columns {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "cols: {:?}", self.names)
  }
}

impl fmt::Debug for Columns {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "cols: {:?} {:?}", self.names, self.indexes)
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
//   fn select(&self, schema: &schema::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
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
//   fn select(&self, schema: &schema::Schema, row: &csv::StringRecord) -> Result<csv::StringRecord, error::Error> {
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
