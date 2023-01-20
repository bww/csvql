mod csvql;
mod error;

use std::io;
use std::fs;
use std::process;

use clap::Parser;

use csvql::query;
use csvql::query::frame::Frame;

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
  
  let mut frm: Option<Box<dyn Frame>> = None;
  let mut sels: Vec<query::Selector> = Vec::new();
  for s in &opts.docs {
    let (name, input): (&str, Box<dyn io::Read>) = if s == "-" {
      ("-", Box::new(io::stdin()))
    }else{
      (&s, Box::new(fs::File::open(&s)?))
    };
    let src = query::frame::Csv::new(&s, input);
    if let Some(opt) = frm {
      frm = Some(Box::new(query::frame::Concat::new(opt, src)));
    }else{
      frm = Some(Box::new(src));
    }
    sels.push(query::Selector::new_with_column(query::Column::new(&s, "hi", 1)));
  }
  
  if let Some(mut frm) = frm {
    println!(">>> {:?}", frm.cols());
    for r in frm.rows() {
      let r = r?;
      let pos = r.position().expect("a record position");
      println!(">>> {} {:?}", pos.line(), r);
    }
  }
  
  // let mut query = query::Query::new(srcs, sels);
  
  Ok(())
}
