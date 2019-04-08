use super::conn::Connection;
use std::net::TcpStream;

pub enum Mode {
    Ingest,
    Search,
    Control,
    NotConnected,
}

pub struct Driver {
    host: String,
    port: usize,
    password: String,
    conn: Option<Connection>,
    pub mode: Mode,
}

impl Driver {
    pub fn new(host: &str, port: usize, password: &str) -> Self {
        Driver {
            host: host.clone().into(),
            port: port,
            password: password.clone().into(),
            conn: None,
            mode: Mode::NotConnected,
        }
    }
    pub fn new_conn(mut self) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(format!("{}:{}", self.host, self.port))?;
        let c = Connection { stream: stream };
        self.conn = Some(c);
        Ok(self)
    }

    pub fn connect(&mut self, chan: &str) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            match conn.write(&format!("START {} {}\n", chan, &self.password)) {
                Ok(_) => {
                    match chan {
                        "ingest" => self.mode = Mode::Ingest,
                        "search" => self.mode = Mode::Search,
                        "control" => self.mode = Mode::Control,
                        // TODO: unsupported channel
                        _ => self.mode = Mode::NotConnected,
                    };
                    conn.read()
                }
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn push(
        &self,
        collection: &str,
        bucket: &str,
        object: &str,
        text: &str,
        locale: Option<&str>,
    ) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!(
                "PUSH {} {} {} \"{}\" {}\n",
                collection,
                bucket,
                object,
                text,
                locale.unwrap_or(""),
            );
            match conn.write(cmd) {
                Ok(_) => conn.read(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn pop(
        &self,
        collection: &str,
        bucket: &str,
        object: &str,
        text: &str,
    ) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!("POP {} {} {} \"{}\"\n", collection, bucket, object, text,);
            match conn.write(cmd) {
                Ok(_) => conn.read(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn count(
        &self,
        collection: &str,
        bucket: &str,
        object: Option<&str>,
        text: Option<&str>,
    ) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!(
                "COUNT {} {} {} {}\n",
                collection,
                bucket,
                object.unwrap_or(""),
                text.unwrap_or(""),
            );
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn flushc(&self, collection: &str) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!("FLUSHC {}\n", collection);
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn flushb(&self, collection: &str, bucket: &str) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!("FLUSHB {} {}\n", collection, bucket);
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn flusho(
        &self,
        collection: &str,
        bucket: &str,
        object: &str,
    ) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!("FLUSHO {} {} {}\n", collection, bucket, object);
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn ping(&self) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            match conn.write("PING\n") {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn help(&self, manual: Option<&str>) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            match conn.write(&format!(
                "HELP{}\n",
                match manual {
                    Some(m) => format!(" {}", m),
                    None => "".to_string(),
                }
            )) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn quit(&self) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            match conn.write("QUIT\n") {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn query(
        &self,
        collection: &str,
        bucket: &str,
        terms: &str,
        limit: Option<&str>,
        offset: Option<&str>,
        lang: Option<&str>,
    ) -> Result<String, std::io::Error> {
        // TODO: add query callback checker
        if let Some(conn) = &self.conn {
            let cmd = &format!(
                "QUERY {} {} \"{}\" {} {} {}\n",
                collection,
                bucket,
                terms,
                limit.unwrap_or(""),
                offset.unwrap_or(""),
                lang.unwrap_or("")
            );
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    pub fn suggest(
        &self,
        collection: &str,
        bucket: &str,
        word: &str,
        limit: Option<&str>,
    ) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!(
                "SUGGEST {} {} \"{}\" {}\n",
                collection,
                bucket,
                word,
                limit.unwrap_or(""),
            );
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    fn trigger(&self, action: Option<&str>) -> Result<String, std::io::Error> {
        if let Some(conn) = &self.conn {
            let cmd = &format!("TRIGGER {}\n", action.unwrap_or(""),);
            match conn.write(cmd) {
                Ok(_) => conn.readline(),
                Err(e) => Err(e),
            }
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[test]
//     fn test_conn() {
//         let mut driver = Driver::new("localhost", 1491, "haha");
//         match driver.new_conn() {
//             Ok(mut d) => {
//                 assert_eq!(
//                     "CONNECTED <sonic-server v1.1.8>\r\nSTARTED ingest protocol(1) buffer(20000)\r\n",
//                     d.connect("ingest").unwrap()
//                 );
//                 assert_eq!(
//                     "OK\r\n",
//                     d.push("messages", "default", "a6b1z", "valerian saliou", None)
//                         .unwrap()
//                 );
//                 assert_eq!(
//                     "RESULT 2\r\n",
//                     d.count("messages", "default", None, None).unwrap()
//                 );
//                 assert_eq!("PONG\r\n", d.ping().unwrap());
//                 assert_eq!("RESULT commands(PUSH, POP, COUNT, FLUSHC, FLUSHB, FLUSHO, PING, HELP, QUIT)\r\n", d.help(Some("commands")).unwrap());
//                 assert_eq!("ENDED quit\r\n", d.quit().unwrap());
//             }
//             Err(e) => print!("Failed to connect: {:?}", e),
//         };
//     }
// }
