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
  
  let mut frms: Vec<query::frame::Csv<Box<dyn io::Read>>> = Vec::new();
  let mut sels: Vec<query::Selector> = Vec::new();
  for s in &opts.docs {
    let (name, input): (&str, Box<dyn io::Read>) = if s == "-" {
      ("-", Box::new(io::stdin()))
    }else{
      (&s, Box::new(fs::File::open(&s)?))
    };
    frms.push(query::frame::Csv::new(&s, input));
    sels.push(query::Selector::new_with_column(query::Column::new(&char::from_u32('a' as u32 + frms.len() as u32).expect("character").to_string(), &s, 1)));
  }
  
  for frm in frms.iter_mut() {
    // if let Some(mut frm) = frm {
      for r in frm.rows() {
        let r = r?;
        let pos = r.position().expect("a record position");
        println!(">>> {} {:?}", pos.line(), r);
      }
    // }
  }
  
  // let mut query = query::Query::new(srcs, sels);
  
  Ok(())
}
