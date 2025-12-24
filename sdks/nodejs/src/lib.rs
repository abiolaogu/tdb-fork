use napi_derive::napi;

#[napi]
pub fn hello() -> String {
  "Hello from LumaDB Node.js SDK".to_string()
}

#[napi]
pub fn sum(a: i32, b: i32) -> i32 {
  a + b
}
