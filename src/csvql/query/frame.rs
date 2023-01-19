use std::io;
use core::iter;

use csv;

use crate::csvql::query::error::Error;

pub trait Frame {
  fn rows<'a, R: io::Read>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a>;
}

#[derive(Debug)]
pub struct Csv<R: io::Read> {
  data: csv::Reader<R>,
}

impl<R: io::Read> Csv<R> {
  pub fn new(data: R) -> Csv<R> {
    Csv{
      data: csv::Reader::from_reader(data),
    }
  }
}

// impl<R: io::Read> Iterator for Csv<R> {
//   type Item = &str;
//   fn next(&mut self) -> Option<Self::Item> {
//     Box::new(csv::Reader::from_reader(&mut self.data).records())
//   }
// }

impl<R: io::Read> Frame for Csv<R> {
  fn rows<'a, S: io::Read>(&'a mut self) -> Box<dyn iter::Iterator<Item = Result<csv::StringRecord, Error>> + 'a> {
    Box::new(self.data.records().map(|e| {
      match e {
        Ok(v)    => Ok(v),
        Err(err) => Err(err.into()),
      }
    }))
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
