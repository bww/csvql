mod csvql;
mod error;

use std::io;
use std::fs;
use std::process;

use clap::Parser;

use csvql::query;
use csvql::query::frame;
use csvql::query::frame::Frame;
use csvql::query::select;
use csvql::query::select::Selector;
use csvql::query::schema;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long, help="Enable verbose output")]
  pub verbose: bool,
  #[clap(long, help="Join inputs on the specified column")]
  pub join: Option<String>,
  #[clap(long="sort:read", help="Sort input on the specified column")]
  pub sort_read: Option<String>,
  #[clap(long="sort:write", help="Sort output data on the specified column")]
  pub sort_write: Option<String>,
  #[clap(long, help="Select columns to report")]
  pub select: Vec<String>,
  #[clap(help="Document to open")]
  pub docs: Vec<String>,
}

fn main() {
  match cmd() {
    Ok(_)     => {},
    Err(err)  => {
      eprintln!("* * * {}", err);
      process::exit(1);
    },
  };
}

fn cmd() -> Result<(), error::Error> {
  let opts = Options::parse();
  
  let mut frms: Vec<Box<dyn Frame>> = Vec::new();
  for s in &opts.docs {
    let (alias, path) = parse_source(&s);
    let (name, input): (&str, Box<dyn io::Read>) = if path == "-" {
      (alias, Box::new(io::stdin()))
    }else{
      (alias, Box::new(fs::File::open(path)?))
    };
    frms.push(Box::new(query::frame::Csv::new(&name, input)?));
  }
  
  let frms = if let Some(on) = &opts.join {
    let join = select::Join::parse(on)?;
    let (mut base, mut base_on): (Option<Box<dyn Frame>>, Option<&schema::QName>) = (None, None);
    for mut frm in frms.into_iter() {
      let on = match join.for_scope(frm.name()) {
        Some(on) => on,
        None => return Err(error::ArgumentError::new(&format!("No join expression matches input frame: {}", frm.name())).into()),
      };
      let sort = select::Sort::on((on.clone(), select::Order::Asc));
      let sort = match &opts.sort_read {
        Some(on) => sort.join(&select::Sort::parse(on)?),
        None => sort,
      };
      if let (Some(curr), Some(curr_on)) = (base, base_on) {
        base = Some(Box::new(frame::OuterJoin::new(curr, curr_on, frame::Sorted::new(&mut frm, &sort)?, on)?));
      }else{
        base = Some(Box::new(frame::Sorted::new(&mut frm, &sort)?));
      }
      base_on = Some(on);
    }
    if let Some(base) = base {
      vec![base]
    }else{
      Vec::new()
    }
  }else{
    frms
  };
  
  for mut frm in frms.into_iter() {
    let frm: Box<dyn Frame> = if let Some(on) = &opts.sort_write {
      Box::new(frame::Sorted::new(&mut frm, &select::Sort::parse(on)?)?)
    }else{
      frm
    };
    
    let sel = if opts.select.len() > 0 {
      Some(select::Columns::parse(frm.schema(), &opts.select)?)
    }else{
      None
    };
    
    let mut frm: Box<dyn Frame> = if let Some(sel) = &sel {
      Box::new(frame::Filter::new(frm, sel.clone())?)
    }else{
      frm
    };
    
    if opts.verbose {
      eprintln!(">>> {}", frm);
    }
    
    let mut dst = csv::Writer::from_writer(io::stdout());
    if let Some(sel) = &sel {
      dst.write_record(&sel.select(&csv::StringRecord::from(frm.schema().record()))?)?;
    }else{
      dst.write_record(frm.schema().record())?;
    }
    
    for row in frm.rows() {
      let row = row?;
      dst.write_record(&row)?;
    }
    
    dst.flush()?;
  }
  
  Ok(())
}

fn parse_source<'a>(f: &'a str) -> (&'a str, &'a str) {
  let split: Vec<&'a str> = f.splitn(2, "=").collect();
  match split.len() {
    2 => (split[0], split[1]),
    1 => (split[0], split[0]),
    _ => ("", ""),
  }
}
