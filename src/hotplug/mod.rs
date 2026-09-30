mod lifecycle;
mod model;
mod runtime;

pub(crate) use runtime::run;

#[cfg(test)]
mod tests;
