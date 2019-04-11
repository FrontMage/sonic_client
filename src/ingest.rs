use super::search::Task;
use mio::net::TcpStream;
use mio::{Events, PollOpt, Ready, Token};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Error, ErrorKind};
use std::mem::drop;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct IngestChan {
    host: String,
    port: usize,
    password: String,
    conn: TcpStream,
    tasks: Arc<Mutex<Vec<Task>>>,
}

const CLIENT: Token = Token(1);

impl IngestChan {
    pub fn new(host: &str, port: usize, password: &str) -> Result<Self, Error> {
        let stream = TcpStream::connect(
            &format!("{}:{}", host, port)
                .parse()
                .expect("Failed to parse socket address"),
        )?;
        let chan = IngestChan {
            host: host.clone().into(),
            port: port,
            password: password.clone().into(),
            conn: stream,
            tasks: Arc::new(Mutex::new(Vec::new())),
        };
        Ok(chan)
    }

    pub fn connect(&mut self) -> Result<String, Error> {
        let msg = format!("START ingest {}\n", &self.password);
        let (task, (_, receiver)) = Task::new(msg.clone());
        {
            let mut t = self.tasks.lock().expect("Failed to acquire task lock");
            t.push(task);
            drop(t);
        }
        let conn = self.conn.try_clone()?;
        let mut writer = BufWriter::new(conn);
        writer.write_all(msg.as_bytes())?;
        receiver.recv().unwrap_or(Ok("".to_string()))
    }

    pub fn read(&mut self) -> thread::JoinHandle<()> {
        let conn = self.conn.try_clone().unwrap();
        let tasks = Arc::clone(&self.tasks);
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
                                                .send(Ok(line.clone()))
                                                .expect("Failed to send msg");
                                        }
                                        drop(t);
                                    } else if line.starts_with("STARTED") {
                                        // Do nothing
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

    pub fn push(
        &mut self,
        collection: &str,
        bucket: &str,
        object: &str,
        text: &str,
        lang: Option<&str>,
    ) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!(
            "PUSH {} {} {} {} {}\r\n",
            collection,
            bucket,
            object,
            format!("\"{}\"", text),
            lang.and_then(|l| Some(format!("LANG({})", l)))
                .unwrap_or("".to_string()),
        ))?;
        Ok(receiver)
    }

    pub fn pop(
        &mut self,
        collection: &str,
        bucket: &str,
        object: &str,
        text: &str,
    ) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!(
            "POP {} {} {} {}\r\n",
            collection,
            bucket,
            object,
            format!("\"{}\"", text),
        ))?;
        Ok(receiver)
    }

    pub fn count(
        &mut self,
        collection: &str,
        bucket: Option<&str>,
        object: Option<&str>,
    ) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!(
            "COUNT {} {} {}\r\n",
            collection,
            bucket.unwrap_or(""),
            object.unwrap_or(""),
        ))?;
        Ok(receiver)
    }

    pub fn flushc(&mut self, collection: &str) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!("FLUSHC {}\r\n", collection,))?;
        Ok(receiver)
    }

    pub fn flushb(
        &mut self,
        collection: &str,
        bucket: &str,
    ) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!("FLUSHB {} {}\r\n", collection, bucket))?;
        Ok(receiver)
    }

    pub fn flusho(
        &mut self,
        collection: &str,
        bucket: &str,
        object: &str,
    ) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) =
            self.write(format!("FLUSHO {} {} {}\r\n", collection, bucket, object))?;
        Ok(receiver)
    }

    pub fn ping(&mut self) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write("PING\r\n".to_string())?;
        Ok(receiver)
    }

    pub fn quit(&mut self) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write("QUIT\r\n".to_string())?;
        Ok(receiver)
    }

    pub fn help(&mut self, manual: Option<&str>) -> Result<Receiver<Result<String, Error>>, Error> {
        let (_, receiver) = self.write(format!("HELP {}\r\n", manual.unwrap_or("")))?;
        Ok(receiver)
    }
}

mod test {
    use super::*;
    use std::time;
    #[test]
    fn test_search() {
        let mut s = IngestChan::new("127.0.0.1", 1491, "haha").expect("Connection error");
        let handle = s.read();
        assert_eq!("CONNECTED <sonic-server v1.1.8>\r\n", s.connect().unwrap());
        thread::sleep(time::Duration::from_secs(4));
        let r1 = s
            .push(
                "messages",
                "user:0dcde3a6",
                "conversation:71f3d63b",
                "Hey Valerian",
                None,
            )
            .unwrap();
        let r2 = s.ping().unwrap();
        let r3 = s.quit().unwrap();
        assert_eq!("OK\r\n", r1.recv().unwrap().unwrap());
        assert_eq!("PONG\r\n", r2.recv().unwrap().unwrap());
        assert_eq!("ENDED quit\r\n", r3.recv().unwrap().unwrap());
        handle.join().expect("Failed to wait process");
    }
}
