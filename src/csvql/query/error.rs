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
pub struct FrameError {
  message: String,
}

impl FrameError {
  pub fn new(msg: &str) -> FrameError {
    FrameError{
      message: msg.to_owned(),
    }
  }
}

impl fmt::Display for FrameError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

#[derive(Debug)]
pub struct QueryError {
  message: String,
}

impl QueryError {
  pub fn new(msg: &str) -> QueryError {
    QueryError{
      message: msg.to_owned(),
    }
  }
}

impl fmt::Display for QueryError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

#[derive(Debug)]
pub enum Error {
  IOError(io::Error),
  CsvError(csv::Error),
  ParseError(ParseError),
  FrameError(FrameError),
  QueryError(QueryError),
  NotFoundError,
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

impl From<FrameError> for Error {
  fn from(err: FrameError) -> Self {
    Self::FrameError(err)
  }
}

impl From<QueryError> for Error {
  fn from(err: QueryError) -> Self {
    Self::QueryError(err)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::IOError(err) => err.fmt(f),
      Self::CsvError(err) => err.fmt(f),
      Self::ParseError(err) => err.fmt(f),
      Self::FrameError(err) => err.fmt(f),
      Self::QueryError(err) => err.fmt(f),
      Self::NotFoundError => write!(f, "not found"),
    }
  }
}
