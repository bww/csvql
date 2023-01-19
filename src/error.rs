use std::io;
use std::fmt;

use crate::csvql::query;

#[derive(Debug)]
pub enum Error {
  IOError(io::Error),
  QueryError(query::error::Error),
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Self {
    Self::IOError(err)
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
      Self::QueryError(err) => err.fmt(f),
    }
  }
}
