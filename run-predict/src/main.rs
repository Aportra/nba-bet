use std::collections::HashMap;

trait Predict {
    fn predict(&self, data: Vec<f64>) -> Vec<f64>;
}

struct Models {
    model_list: Vec<Box<dyn Predict>>,
    output: HashMap<String, f64>,
}

fn main() {
    println!("Hello, world!");
}
