mod csvql;
mod error;

use std::io;
use std::fs;
use std::process;

use clap::Parser;

use csvql::query;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(help="Document to open")]
  pub docs: Vec<String>,
}

fn main() {
  match cmd() {
    Ok(_)     => {},
    Err(err)  => {
      println!("* * * {}", err);
      process::exit(1);
    },
  };
}

fn cmd() -> Result<(), error::Error> {
  let opts = Options::parse();
  println!("Hello, world! {:?}", opts);
  
  let mut srcs: Vec<query::Source<Box<dyn query::Frame>>> = Vec::new();
  for s in &opts.docs {
    let (name, input): (&str, Box<dyn io::Read>) = if s == "-" {
      ("stdin", Box::new(io::stdin()))
    }else{
      (&s, Box::new(fs::OpenOptions::new().open(&s)?))
    };
    srcs.push(query::Source::new_with_data(name, Box::new(query::InputFrame::new(input))));
  }
  
  let mut sels: Vec<query::Selector> = Vec::new();
  for s in &srcs {
    sels.push(query::Selector::new_with_column(query::Column::new(s.name(), "hi", 1)));
  }
  
  let query = query::Query::new(srcs, sels);
  
  Ok(())
}
