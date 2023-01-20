use std::io;
use core::iter;

use csv;

use crate::csvql::query::error::Error;

fn convert_record(e: csv::Result<csv::StringRecord>) -> Result<csv::StringRecord, Error> {
  match e {
    Ok(v)    => Ok(v),
    Err(err) => Err(err.into()),
  }
}

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

#[derive(Debug)]
pub struct Csv<R: io::Read> {
  name: String,
  data: csv::Reader<R>,
}

impl<R: io::Read> Csv<R> {
  pub fn new(name: &str, data: R) -> Csv<R> {
    Csv{
      name: name.to_owned(),
      data: csv::ReaderBuilder::new().has_headers(true).from_reader(data),
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

// #[derive(Debug, Clone)]
// pub struct Join<L: Frame, R: Frame> {
//   left:  L,
//   right: R,
// }

// impl<L: Frame, R: Frame> Join<L, R> {
//   pub fn new(left: L, right: R) -> Join<L, R> {
//     Join{
//       left: left,
//       right: right,
//     }
//   }
// }

// impl<L: Frame, R: Frame> Frame for Join<L, R> {
//   fn rows(&mut self) {
//     // ...
//   }
// }
