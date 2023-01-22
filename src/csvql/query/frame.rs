use core::iter;

use std::io;
use std::fmt;
use std::collections::BTreeMap;

use csv;

use crate::csvql::query::error::Error;

fn convert_record(e: csv::Result<csv::StringRecord>) -> Result<csv::StringRecord, Error> {
  match e {
    Ok(v)    => Ok(v),
    Err(err) => Err(err.into()),
  }
}

// A frame of data
pub trait Frame {
  fn name<'a>(&'a self) -> &str;
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a>;
}

impl<F: Frame + ?Sized> Frame for Box<F> { // black magic
  fn name<'a>(&'a self) -> &str {
    (**self).name()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    (**self).rows()
  }
}

// A random-access frame indexed on a particular column
pub trait Index: Frame {
  fn on<'a>(&'a self) -> &'a str; // the indexed column
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, Error>;
}

// A btree indexed frame
#[derive(Debug)]
pub struct BTreeIndex {
  name: String,
  on: String,
  data: BTreeMap<String, csv::StringRecord>,
}

impl BTreeIndex {
  pub fn new(name: &str, on: &str, source: &dyn Frame) -> Result<BTreeIndex, Error> {
    let data: BTreeMap<String, csv::StringRecord> = BTreeMap::new();
    BTreeIndex{
      name: name.to_owned(),
      on: on.to_owned(),
      data: Self::index(source)?,
    }
  }
  
  fn index(source: &dyn Frame) -> Result<BTreeMap<String, csv::StringRecord>, Error> {
    let data BTreeMap<String, csv::StringRecord> = BTreeMap::new();
    data
  }
}

impl Frame for BTreeIndex {
  fn name<'a>(&'a self) -> &str {
    &self.name
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    Box::new(self.data.records().map(|e| { convert_record(e) }))
  }
}

impl<R: io::Read> fmt::Display for BTreeIndex {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.name)
  }
}

// A CSV input frame
#[derive(Debug)]
pub struct Csv<R: io::Read> {
  name: String,
  data: csv::Reader<R>,
}

impl<R: io::Read> Csv<R> {
  pub fn new(name: &str, data: R) -> Csv<R> {
    Csv{
      name: name.to_owned(),
      data: csv::ReaderBuilder::new().has_headers(false).from_reader(data),
    }
  }
}

impl<R: io::Read> Frame for Csv<R> {
  fn name<'a>(&'a self) -> &str {
    &self.name
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    Box::new(self.data.records().map(|e| { convert_record(e) }))
  }
}

impl<R: io::Read> fmt::Display for Csv<R> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.name)
  }
}

// A frame that concatenates the output of two other frames
#[derive(Debug)]
pub struct Concat<A: Frame, B: Frame> {
  first: A,
  second: B,
}

impl<A: Frame, B: Frame> Concat<A, B> {
  pub fn new(first: A, second: B) -> Concat<A, B> {
    Concat{
      first: first,
      second: second,
    }
  }
}

impl<L: Frame, R: Frame> Frame for Concat<L, R> {
  fn name<'a>(&'a self) -> &str {
    self.first.name()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    Box::new( self.first.rows().chain(self.second.rows()))
  }
}

// A frame that left-joins two frames on an indexed column
#[derive(Debug)]
pub struct Join<L: Frame, R: Index> {
  on: String,
  left:  L,
  right: R,
}

impl<L: Frame, R: Index> Frame for Join<L, R> {
  fn name<'a>(&'a self) -> &str {
    self.left.name()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    Box::new( self.left.rows().chain(self.right.rows()))
  }
}
