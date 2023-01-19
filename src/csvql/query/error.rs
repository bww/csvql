use std::io;
use std::fmt;

use csv;

#[derive(Debug)]
pub enum Error {
  IOError(io::Error),
  CsvError(csv::Error),
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Self {
    Self::IOError(err)
  }
}

impl From<csv::Error> for Error {
  fn from(err: csv::Error) -> Self {
    Self::CsvError(err)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::IOError(err) => err.fmt(f),
      Self::CsvError(err) => err.fmt(f),
    }
  }
}
