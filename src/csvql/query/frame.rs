use core::iter;

use std::io;
use std::fmt;
use std::collections::HashMap;
use std::collections::BTreeMap;

use csv;

use crate::csvql::query::error;

fn convert_record(e: csv::Result<csv::StringRecord>) -> Result<csv::StringRecord, error::Error> {
  match e {
    Ok(v)    => Ok(v),
    Err(err) => Err(err.into()),
  }
}

pub struct Schema {
  cols: HashMap<String, usize>,
}

impl Schema {
  pub fn new_from_headers(hdrs: &csv::StringRecord) -> Schema {
    let mut cols: HashMap<String, usize> = HashMap::new();
    let mut n: usize = 0;
    for hdr in hdrs {
      cols.insert(hdr.to_string(), n);
      n += 1;
    }
    Schema{
      cols: cols,
    }
  }
  
  pub fn index(&self, name: &str) -> Option<usize> {
    match self.cols.get(name) {
      Some(index) => Some(*index),
      None => None,
    }
  }
}

impl fmt::Display for Schema {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut dsc = String::new();
    let mut n = 0;
    for key in self.cols.keys() {
      if n > 0 {
        dsc.push_str(", ");
      }
      dsc.push_str(&key);
      n += 1;
    }
    write!(f, "columns: {}", dsc)
  }
}

// A frame of data
pub trait Frame {
  fn name<'a>(&'a self) -> &str;
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a>;
}

impl<F: Frame + ?Sized> Frame for Box<F> { // black magic
  fn name<'a>(&'a self) -> &str {
    (**self).name()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    (**self).rows()
  }
}

// A random-access frame indexed on a particular column
pub trait Index: Frame {
  fn on<'a>(&'a self) -> &'a str; // the indexed column
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error>;
}

impl<I: Index + ?Sized> Index for Box<I> { // black magic
  fn on<'a>(&'a self) -> &'a str {
    (**self).on()
  }
  
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error> {
    (**self).get(key)
  }
}

// A btree indexed frame
#[derive(Debug)]
pub struct BTreeIndex {
  name: String,
  on: String,
  data: BTreeMap<String, csv::StringRecord>,
}

impl BTreeIndex {
  pub fn new(name: &str, on: &str, source: &mut dyn Frame) -> Result<BTreeIndex, error::Error> {
    Ok(BTreeIndex{
      name: name.to_owned(),
      on: on.to_owned(),
      data: Self::index(on, source)?,
    })
  }
  
  fn index(on: &str, source: &mut dyn Frame) -> Result<BTreeMap<String, csv::StringRecord>, error::Error> {
    let mut data: BTreeMap<String, csv::StringRecord> = BTreeMap::new();
    
    let mut it = source.rows();
    let schema = if let Some(hdrs) = it.next() {
      Schema::new_from_headers(&hdrs?)
    }else{
      return Ok(data); // empty source
    };
    
    let index = match schema.index(on) {
      Some(index) => index,
      None => return Err(error::FrameError::new("Index column not found").into()),
    };
    for row in it {
      let row = row?;
      match row.get(index) {
        Some(col) => data.insert(col.to_string(), row),
        None => /* row is omitted */ None,
      };
    }
    
    Ok(data)
  }
}

impl Frame for BTreeIndex {
  fn name<'a>(&'a self) -> &str {
    &self.name
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new(self.data.values().map(|e| { Ok(e.to_owned()) }))
  }
}

impl Index for BTreeIndex {
  fn on<'a>(&'a self) -> &'a str {
    &self.on
  }
  
  fn get<'a>(&'a self, key: &str) -> Result<&'a csv::StringRecord, error::Error> {
    match self.data.get(key) {
      Some(val) => Ok(&val),
      None => Err(error::Error::NotFoundError.into()),
    }
  }
}

impl fmt::Display for BTreeIndex {
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
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
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
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
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

impl<L: Frame, R: Index> Join<L, R> {
  pub fn new(on: &str, left: L, right: R) -> Join<L, R> {
    Join{
      on: on.to_string(),
      left: left,
      right: right,
    }
  }
}

impl<L: Frame, R: Index> Frame for Join<L, R> {
  fn name<'a>(&'a self) -> &str {
    self.left.name()
  }
  
  fn rows<'a>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, error::Error>> + 'a> {
    Box::new( self.left.rows().chain(self.right.rows()))
  }
}
