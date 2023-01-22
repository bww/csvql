use std::io;
use std::fmt;

use csv;

use crate::csvql::query;

#[derive(Debug)]
pub struct ArgumentError {
  message: String,
}

impl fmt::Display for ArgumentError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

#[derive(Debug)]
pub enum Error {
  IOError(io::Error),
  CsvError(csv::Error),
  ArgumentError(ArgumentError),
  QueryError(query::error::Error),
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

impl From<ArgumentError> for Error {
  fn from(err: ArgumentError) -> Self {
    Self::ArgumentError(err)
  }
}

impl From<query::error::Error> for Error {
  fn from(err: query::error::Error) -> Self {
    Self::QueryError(err)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::IOError(err) => err.fmt(f),
      Self::CsvError(err) => err.fmt(f),
      Self::ArgumentError(err) => err.fmt(f),
      Self::QueryError(err) => err.fmt(f),
    }
  }
}
