# sonic_client

Rust client for [sonic](https://github.com/valeriansaliou/sonic), a Rust based search platform.

# Note!

This lib is under development, apis could change very fast.

## Usage

```toml
sonic_client = "0.1.1"
```

```rust
use sonic_client::SearchChan;

let mut s = SearchChan::new("127.0.0.1", 1491, "haha").expect("Connection error");
let handle = s.read();
assert_eq!("CONNECTED <sonic-server v1.1.8>\r\n", s.connect().unwrap());
thread::sleep(time::Duration::from_secs(4));
let r1 = s
    .query("helpdesk", "user:0dcde3a6", "gdpr", Some(50), None)
    .unwrap();
let r2 = s.ping().unwrap();
let r3 = s.quit().unwrap();
assert_eq!("EVENT", r1[0]);
assert_eq!("PONG\r\n", r2.recv().unwrap());
assert_eq!("ENDED quit\r\n", r3.recv().unwrap());
handle.join().expect("Failed to wait process");
```

## Roadmap

- Respect buffer 20000 limit
- Better error parsing
- pooling
- better debug mode message
