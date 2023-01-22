use std::io;
use std::fmt;

use csv;

#[derive(Debug)]
pub struct ParseError {
  message: String,
}

impl ParseError {
  pub fn new(msg: &str) -> ParseError {
    ParseError{
      message: msg.to_owned(),
    }
  }
}

impl fmt::Display for ParseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

#[derive(Debug)]
pub enum Error {
  IOError(io::Error),
  CsvError(csv::Error),
  ParseError(ParseError),
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

impl From<ParseError> for Error {
  fn from(err: ParseError) -> Self {
    Self::ParseError(err)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::IOError(err) => err.fmt(f),
      Self::CsvError(err) => err.fmt(f),
      Self::ParseError(err) => err.fmt(f),
    }
  }
}
