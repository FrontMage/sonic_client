use super::conn::Connection;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::TcpStream;
use tokio;

pub enum Status {
    Pending,
    Resolved,
    Rejected,
}

pub struct Task {
    msg: String,
    status: Status,
    result: String,
    err: Option<std::io::Error>,
}

impl Future for Task {
    type Item = String;
    type Error = std::io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.status {
            Status::Pending => Ok(Async::NotReady),
            Status::Resolved => Ok(Async::Ready(self.result.clone())),
            Status::Rejected => {
                let e = self.err.take().unwrap();
                Err(e)
            }
        }
    }
}

pub struct SearchChan {
    host: String,
    port: usize,
    password: String,
    conn: Option<Connection>,
    search_ids: HashMap<String, Task>,
    tasks: Vec<Task>,
}

impl SearchChan {
    pub fn new(host: &str, port: usize, password: &str) -> Result<Self, std::io::Error> {
        let mut chan = SearchChan {
            host: host.clone().into(),
            port: port,
            password: password.clone().into(),
            conn: None,
            search_ids: HashMap::new(),
            tasks: Vec::new(),
        };
        let stream = TcpStream::connect(format!("{}:{}", chan.host, chan.port))?;
        let c = Connection { stream: stream };
        chan.conn = Some(c);
        if let Some(conn) = &chan.conn {
            match conn.write(&format!("START search {}\n", &chan.password)) {
                Ok(_) => {
                    conn.read()?;
                    Ok(chan)
                }
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn new_task(&mut self, task: Task) {
        self.tasks.push(task);
    }
}

impl Future for SearchChan {
    type Item = ();
    type Error = std::io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match &self.conn {
            Some(conn) => {
                if let Some(mut t) = self.tasks.pop() {
                    if let Err(e) = conn.write(&t.msg) {
                        return Err(e);
                    }
                    return match conn.readline() {
                        Ok(line) => {
                            if line.starts_with("PENDING") {
                                &self.search_ids.insert(
                                    line.trim_start_matches("PENDING ").trim().to_string(),
                                    t,
                                );
                            } else {
                                t.result = line.clone();
                                t.status = Status::Resolved;
                                t.poll()?;
                            }
                            Ok(Async::NotReady)
                        }
                        Err(e) => {
                            t.status = Status::Rejected;
                            t.err = Some(e);
                            t.poll()?;
                            Ok(Async::NotReady)
                        }
                    };
                } else {
                    let line = conn.readline()?;
                    if line.starts_with("EVENT QUERY") {
                        let results: Vec<&str> = line
                            .trim_start_matches("EVENT QUERY")
                            .trim()
                            .split(" ")
                            .collect();
                        let id = results[0];
                        if let Some(mut t) = self.search_ids.remove(id) {
                            t.result = results[1..].join(" ");
                            t.status = Status::Resolved;
                            t.poll()?;
                        }
                    }
                    Ok(Async::NotReady)
                }
            }
            None => Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        }
    }
}

struct Ping(Task);

impl Future for Ping {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<(), ()> {
        match self.0.status {
            Status::Pending => Ok(Async::NotReady),
            Status::Resolved => {
                println!("{}", self.0.result);
                Ok(Async::Ready(()))
            }
            Status::Rejected => Ok(Async::Ready(())),
        }
    }
}

pub struct Runner(SearchChan);

impl Future for Runner {
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<(), ()> {
        self.0.poll().unwrap();
        Ok(Async::NotReady)
    }
}

mod test {
    use super::*;
    #[test]
    fn test_search() {
        let mut s = SearchChan::new("localhost", 1491, "haha").unwrap();
        let t = Task {
            msg: "QUERY helpdesk user:0dcde3a6 \"gdpr\" LIMIT(50)\n".to_string(),
            result: String::new(),
            status: Status::Pending,
            err: None,
        };
        s.new_task(t);
        loop {
            s.poll();
            println!("{:?}", s.search_ids.keys());
        }
    }
}
