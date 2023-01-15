mod csvql;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Options {
  #[clap(long, help="Enable debugging mode")]
  pub debug: bool,
  #[clap(long)]
  pub verbose: bool,
  #[clap(help="Document to open")]
  pub doc: Vec<String>,
}

fn main() {
  let opts = Options::parse();
  println!("Hello, world! {:?}", opts);
}
