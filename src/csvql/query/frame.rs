use std::io;

use csv;

pub trait Frame {
  fn rows(&mut self);
}

#[derive(Debug, Clone)]
pub struct Csv<R: io::Read> {
  data: R,
}

impl<R: io::Read> Csv<R> {
  pub fn new(data: R) -> Csv<R> {
    Csv{
      data: data,
    }
  }
}

impl<R: io::Read> Frame for Csv<R> {
  fn rows(&mut self) {
    let _ = csv::Reader::from_reader(&mut self.data).records();
  }
}

#[derive(Debug, Clone)]
pub struct Join<L: Frame, R: Frame> {
  left:  L,
  right: R,
}

impl<L: Frame, R: Frame> Join<L, R> {
  pub fn new(left: L, right: R) -> Join<L, R> {
    Join{
      left: left,
      right: right,
    }
  }
}

impl<L: Frame, R: Frame> Frame for Join<L, R> {
  fn rows(&mut self) {
    // ...
  }
}
