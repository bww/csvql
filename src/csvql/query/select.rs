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
  on: Vec<schema::QName>,
}

impl Join {
  pub fn parse(text: &str) -> Result<Join, error::Error> {
    let mut on: Vec<schema::QName> = Vec::new();
    for e in text.split("=") {
      on.push(schema::QName::parse(e)?);
    }
    Ok(Self::new(on))
  }
  
  pub fn new(on: Vec<schema::QName>) -> Join {
    Join{
      on: on,
    }
  }
  
  pub fn for_scope<'a>(&'a self, scope: &str) -> Option<&'a schema::QName> {
    let check = Some(scope);
    for e in &self.on {
      if e.scope() == check {
        return Some(&e);
      }
    }
    None
  }
}

impl fmt::Display for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "<{:?}>", &self.on)
  }
}

impl fmt::Debug for Join {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "<{:?}>", &self.on)
  }
}
