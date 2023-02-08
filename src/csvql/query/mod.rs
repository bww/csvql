pub mod frame;
pub mod select;
pub mod schema;
pub mod error;

// use frame::Frame;

// #[derive(Debug, Clone)]
// pub struct Query<F: Frame> {
//   sources: Vec<Source<F>>,
//   // selectors: Vec<Selector>,
// }

// impl<F: Frame> Query<F> {
//   // pub fn new(sources: Vec<Source<F>>, selectors: Vec<Selector>) -> Query<F> {
//   pub fn new(sources: Vec<Source<F>>) -> Query<F> {
//     Query{
//       sources: sources,
//       // selectors: selectors,
//     }
//   }
// }

// #[derive(Debug, Clone)]
// pub struct Source<F: Frame> {
//   name: String,
//   data: F,
// }

// impl<F: Frame> Source<F> {
//   pub fn new_with_data(name: &str, data: F) -> Source<F> {
//     Source{
//       name: name.to_owned(),
//       data: data,
//     }
//   }
  
//   pub fn name<'a>(&'a self) -> &'a str {
//     &self.name
//   }
  
//   pub fn data<'a>(&'a self) -> &'a F {
//     &self.data
//   }
// }
