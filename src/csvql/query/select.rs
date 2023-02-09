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
  names: Vec<schema::QName>,
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
    let names: Vec<schema::QName> = qnames.clone();
    let mut indexes: Vec<usize> = Vec::new();
    for qname in qnames {
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

#[derive(Clone)]
pub struct Join {
  left: schema::QName,
  right: schema::QName,
}

impl Join {
  pub fn parse(text: &str) -> Result<Join, error::Error> {
    let split: Vec<&str> = text.splitn(2, "=").collect();
    match split.len() {
      2 => Ok(Self::new(
        &schema::QName::parse(split[0])?,
        &schema::QName::parse(split[1])?
      )),
      1 => match schema::QName::parse(split[0]) {
        Ok(qname) => Ok(Self::new(&qname, &qname)),
        Err(err) => Err(err.into()),
      },
      _ => Err(error::ParseError::new(&format!("Invalid qname format: {}", text)).into()),
    }
  }
  
  pub fn new(left: &schema::QName, right: &schema::QName) -> Join {
    Join{
      left: left.clone(),
      right: right.clone(),
    }
  }
  
  pub fn left<'a>(&'a self) -> &'a schema::QName {
    &self.left
  }
  
  pub fn right<'a>(&'a self) -> &'a schema::QName {
    &self.right
  }
}

impl fmt::Display for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}<>{}", &self.left, &self.right)
  }
}

impl fmt::Debug for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}<>{:?}", &self.left, &self.right)
  }
}
