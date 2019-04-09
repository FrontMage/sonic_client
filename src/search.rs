extern crate mio;
use mio::net::TcpStream;
use mio::{Events, PollOpt, Ready, Token};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct Task {
    pub msg: String,
    pub sender: SyncSender<String>,
}

impl Task {
    pub fn new(msg: String) -> (Self, (SyncSender<String>, Receiver<String>)) {
        let (sender, receiver) = sync_channel::<String>(2);
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
    search_ids: Arc<Mutex<HashMap<String, SyncSender<String>>>>,
    tasks: Arc<Mutex<Vec<Task>>>,
}

const CLIENT: Token = Token(0);

impl SearchChan {
    pub fn new(host: &str, port: usize, password: &str) -> Result<Self, std::io::Error> {
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

    pub fn connect(&mut self) -> Result<String, std::io::Error> {
        let msg = format!("START search {}\n", &self.password);
        println!("{}", msg);
        let (task, (_, receiver)) = Task::new(msg.clone());
        {
            let mut t = self.tasks.lock().expect("Failed to acquire task lock");
            t.push(task);
        }
        let conn = self.conn.try_clone()?;
        let mut writer = BufWriter::new(conn);
        writer.write_all(msg.as_bytes())?;
        Ok(receiver.recv().unwrap_or("".to_string()))
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
                                        // TODO: deal error
                                    } else if line.starts_with("CONNECTED") {
                                        let mut t = tasks.lock().unwrap();
                                        if t.len() > 0 {
                                            let task = t.remove(0);
                                            task.sender
                                                .send(line.clone())
                                                .expect("Failed to send msg");
                                        }
                                    } else if line.starts_with("STARTED") {
                                        // Do nothing
                                    } else if line.starts_with("EVENT") {
                                        let tokens = line.split(" ").collect::<Vec<&str>>();
                                        let id = tokens[2];
                                        let mut ids = search_ids
                                            .lock()
                                            .expect("Failed to acquire search_ids lock");
                                        if let Some(sender) = ids.remove(id) {
                                            sender
                                                .send(tokens[3..].join(" "))
                                                .expect("Failed to send event");
                                        }
                                    } else {
                                        let mut t = tasks.lock().unwrap();
                                        if t.len() > 0 {
                                            let task = t.remove(0);
                                            task.sender
                                                .send(line.clone())
                                                .expect("Failed to send msg");
                                        }
                                    }
                                    if line.starts_with("ENDED") {
                                        break 'event_loop;
                                    }
                                    line = String::new();
                                }
                            }
                            Err(e) => {
                                if e.kind() != std::io::ErrorKind::WouldBlock {
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
    ) -> Result<(SyncSender<String>, Receiver<String>), std::io::Error> {
        let (task, (sender, receiver)) = Task::new(msg.clone());
        {
            let mut t = self.tasks.lock().expect("Failed to acquire task lock");
            t.push(task);
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
    ) -> Result<Receiver<String>, std::io::Error> {
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
        if let Ok(id) = receiver.recv() {
            let mut search_ids = self
                .search_ids
                .lock()
                .expect("Failed to acquire search_id lock");
            search_ids.insert(
                id.split(" ").collect::<Vec<&str>>()[1].trim().to_string(),
                sender,
            );
        }
        Ok(receiver)
    }

    // TODO: check if suggest id conflicts with search
    pub fn suggest(
        &mut self,
        collection: &str,
        bucket: &str,
        word: &str,
        limit: Option<i32>,
    ) -> Result<Receiver<String>, std::io::Error> {
        let (sender, receiver) = self.write(format!(
            "SUGGEST {} {} {} {}\r\n",
            collection,
            bucket,
            format!("\"{}\"", word),
            limit
                .and_then(|l| Some(format!("LIMIT({})", l)))
                .unwrap_or("".to_string()),
        ))?;
        if let Ok(id) = receiver.recv() {
            let mut search_ids = self
                .search_ids
                .lock()
                .expect("Failed to acquire search_id lock");
            search_ids.insert(
                id.split(" ").collect::<Vec<&str>>()[1].trim().to_string(),
                sender,
            );
        }
        Ok(receiver)
    }

    pub fn ping(&mut self) -> Result<Receiver<String>, std::io::Error> {
        let (_, receiver) = self.write("PING\r\n".to_string())?;
        Ok(receiver)
    }

    pub fn quit(&mut self) -> Result<Receiver<String>, std::io::Error> {
        let (_, receiver) = self.write("QUIT\r\n".to_string())?;
        Ok(receiver)
    }

    pub fn help(&mut self, manual: Option<&str>) -> Result<Receiver<String>, std::io::Error> {
        let (_, receiver) = self.write(format!("HELP {}\r\n", manual.unwrap_or("")))?;
        Ok(receiver)
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
        let r2 = s.ping().unwrap();
        let r3 = s.quit().unwrap();
        assert_eq!("\r\n", r1.recv().unwrap());
        assert_eq!("PONG\r\n", r2.recv().unwrap());
        assert_eq!("ENDED quit\r\n", r3.recv().unwrap());
        handle.join().expect("Failed to wait process");
    }
}
