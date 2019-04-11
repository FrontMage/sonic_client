use mio::net::TcpStream;
use mio::{Events, PollOpt, Ready, Token};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Error, ErrorKind};
use std::mem::drop;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Task {
    pub msg: String,
    pub sender: SyncSender<Result<String, Error>>,
}

impl Task {
    pub fn new(
        msg: String,
    ) -> (
        Self,
        (
            SyncSender<Result<String, Error>>,
            Receiver<Result<String, Error>>,
        ),
    ) {
        let (sender, receiver) = sync_channel::<Result<String, Error>>(2);
        (
            Task {
                msg: msg,
                sender: sender.clone(),
            },
            (sender, receiver),
        )
    }
}

pub struct SearchChan {
    host: String,
    port: usize,
    password: String,
    conn: TcpStream,
    search_ids: Arc<Mutex<HashMap<String, SyncSender<Result<String, Error>>>>>,
    tasks: Arc<Mutex<Vec<Task>>>,
}

const CLIENT: Token = Token(0);

impl SearchChan {
    pub fn new(host: &str, port: usize, password: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(
            &format!("{}:{}", host, port)
                .parse()
                .expect("Failed to parse socket address"),
        )?;
        let chan = SearchChan {
            host: host.clone().into(),
            port: port,
            password: password.clone().into(),
            conn: stream,
            search_ids: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
        };
        Ok(chan)
    }

    pub fn connect(&mut self) -> Result<String, Error> {
        let msg = format!("START search {}\n", &self.password);
        let (task, (_, receiver)) = Task::new(msg.clone());
        {
            let mut t = self.tasks.lock().expect("Failed to acquire task lock");
            t.push(task);
            drop(t);
        }
        let conn = self.conn.try_clone()?;
        let mut writer = BufWriter::new(conn);
        writer.write_all(msg.as_bytes())?;
        receiver
            .recv()
            .or_else(|e| Err(Error::new(ErrorKind::Other, format!("{:?}", e))))
            .unwrap()
    }

    pub fn read(&mut self) -> thread::JoinHandle<()> {
        let conn = self.conn.try_clone().unwrap();
        let tasks = Arc::clone(&self.tasks);
        let search_ids = Arc::clone(&self.search_ids);
        thread::spawn(move || {
            let poll = mio::Poll::new().unwrap();
            poll.register(&conn, CLIENT, Ready::readable(), PollOpt::edge())
                .unwrap();
            let mut events = Events::with_capacity(1024);
            let mut reader = BufReader::new(&conn);
            let mut line = String::new();
            'event_loop: loop {
                poll.poll(&mut events, None).unwrap();
                for event in events.iter() {
                    match event.token() {
                        CLIENT => match reader.read_line(&mut line) {
                            Ok(_) => {
                                if line.ends_with("\r\n") {
                                    if line.starts_with("ERR") {
                                        let mut t = tasks.lock().unwrap();
                                        if t.len() > 0 {
                                            let task = t.remove(0);
                                            task.sender
                                                .send(Ok(line.clone()))
                                                .expect("Failed to send msg err");
                                        }
                                        drop(t);
                                    } else if line.starts_with("CONNECTED") {
                                        let mut t = tasks.lock().unwrap();
                                        if t.len() > 0 {
                                            let task = t.remove(0);
                                            task.sender
                                                .send(Ok(line.clone()))
                                                .expect("Failed to send msg connected");
                                        }
                                        drop(t);
                                    } else if line.starts_with("STARTED") {
                                        // Do nothing, connection is started after CONNECTED
                                    } else if line.starts_with("EVENT") {
                                        let tokens = line.split(" ").collect::<Vec<&str>>();
                                        let id = tokens[2];
                                        let mut ids = search_ids
                                            .lock()
                                            .expect("Failed to acquire search_ids lock");
                                        if let Some(sender) = ids.remove(id) {
                                            sender
                                                .send(Ok(tokens.join(" ")))
                                                .expect("Failed to send event");
                                        }
                                        drop(ids);
                                    } else {
                                        let mut t = tasks.lock().unwrap();
                                        if t.len() > 0 {
                                            let task = t.remove(0);
                                            task.sender
                                                .send(Ok(line.clone()))
                                                .expect("Failed to send msg");
                                        }
                                        drop(t);
                                    }
                                    if line.starts_with("ENDED") {
                                        break 'event_loop;
                                    }
                                    line = String::new();
                                }
                            }
                            Err(e) => {
                                if e.kind() != ErrorKind::WouldBlock {
                                    println!("{:?}", e);
                                }
                            }
                        },
                        _ => unreachable!(),
                    };
                }
            }
        })
    }

    pub fn write(
        &mut self,
        msg: String,
    ) -> Result<
        (
            SyncSender<Result<String, Error>>,
            Receiver<Result<String, Error>>,
        ),
        Error,
    > {
        let (task, (sender, receiver)) = Task::new(msg.clone());
        {
            let mut t = self.tasks.lock().expect("Failed to acquire task lock");
            t.push(task);
            drop(t);
        }
        let conn = self.conn.try_clone()?;
        let mut writer = BufWriter::new(conn);
        writer.write_all(msg.as_bytes())?;
        Ok((sender.clone(), receiver))
    }

    pub fn query(
        &mut self,
        collection: &str,
        bucket: &str,
        terms: &str,
        limit: Option<i32>,
        offset: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let (sender, receiver) = self.write(format!(
            "QUERY {} {} {} {} {}\r\n",
            collection,
            bucket,
            format!("\"{}\"", terms),
            limit
                .and_then(|l| Some(format!("LIMIT({})", l)))
                .unwrap_or("".to_string()),
            offset
                .and_then(|l| Some(format!("OFFSET({})", l)))
                .unwrap_or("".to_string()),
        ))?;
        if let Ok(res) = receiver.recv() {
            if let Ok(id) = res {
                let mut search_ids = self
                    .search_ids
                    .lock()
                    .expect("Failed to acquire search_id lock");
                search_ids.insert(
                    id.split(" ").collect::<Vec<&str>>()[1].trim().to_string(),
                    sender,
                );
                drop(search_ids);
            }
        }
        match receiver.recv() {
            Ok(result) => match result {
                Ok(res) => Ok(res
                    .split(" ")
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()),
                Err(e) => Err(e),
            },
            Err(_) => Err(Error::new(ErrorKind::Other, "Failed to receive from query")),
        }
    }

    pub fn suggest(
        &mut self,
        collection: &str,
        bucket: &str,
        word: &str,
        limit: Option<i32>,
    ) -> Result<Vec<String>, Error> {
        let (sender, receiver) = self.write(format!(
            "SUGGEST {} {} {} {}\r\n",
            collection,
            bucket,
            format!("\"{}\"", word),
            limit
                .and_then(|l| Some(format!("LIMIT({})", l)))
                .unwrap_or("".to_string()),
        ))?;
        if let Ok(res) = receiver.recv() {
            if let Ok(id) = res {
                let mut search_ids = self
                    .search_ids
                    .lock()
                    .expect("Failed to acquire search_id lock");
                search_ids.insert(
                    id.split(" ").collect::<Vec<&str>>()[1].trim().to_string(),
                    sender,
                );
                drop(search_ids);
            }
        }
        match receiver.recv() {
            Ok(result) => match result {
                Ok(res) => Ok(res
                    .split(" ")
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()),
                Err(e) => Err(e),
            },
            Err(_) => Err(Error::new(ErrorKind::Other, "Failed to receive from query")),
        }
    }

    pub fn ping(&mut self) -> Result<String, Error> {
        let (_, receiver) = self.write("PING\r\n".to_string())?;
        receiver
            .recv()
            .or_else(|e| Err(Error::new(ErrorKind::Other, format!("{:?}", e))))
            .unwrap()
    }

    pub fn quit(&mut self) -> Result<String, Error> {
        let (_, receiver) = self.write("QUIT\r\n".to_string())?;
        receiver
            .recv()
            .or_else(|e| Err(Error::new(ErrorKind::Other, format!("{:?}", e))))
            .unwrap()
    }

    pub fn help(&mut self, manual: Option<&str>) -> Result<String, Error> {
        let (_, receiver) = self.write(format!("HELP {}\r\n", manual.unwrap_or("")))?;
        receiver
            .recv()
            .or_else(|e| Err(Error::new(ErrorKind::Other, format!("{:?}", e))))
            .unwrap()
    }
}

mod test {
    use super::*;
    use std::time;
    #[test]
    fn test_search() {
        let mut s = SearchChan::new("127.0.0.1", 1491, "haha").expect("Connection error");
        let handle = s.read();
        assert_eq!("CONNECTED <sonic-server v1.1.8>\r\n", s.connect().unwrap());
        thread::sleep(time::Duration::from_secs(4));
        let r1 = s
            .query("helpdesk", "user:0dcde3a6", "gdpr", Some(50), None)
            .unwrap();
        let r2 = s.ping();
        let r3 = s.quit();
        assert_eq!("EVENT", r1[0]);
        assert_eq!("PONG\r\n", r2.unwrap());
        assert_eq!("ENDED quit\r\n", r3.unwrap());
        handle.join().expect("Failed to wait process");
    }
}
