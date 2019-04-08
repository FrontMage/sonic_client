use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::TcpStream;
use std::time;

pub struct Connection {
    pub stream: TcpStream,
}

impl Connection {
    // Read until "OK" or "ERR" or "ENDED" or "STARTED"
    pub fn read(&self) -> Result<String, std::io::Error> {
        let mut result = String::new();
        loop {
            if let Ok(line) = self.readline() {
                result = format!("{}{}", &result, line);
            } else {
                println!("Timeout");
                break;
            }
        }
        Ok(result)
    }
    pub fn readline(&self) -> Result<String, std::io::Error> {
        &self
            .stream
            .set_read_timeout(Some(time::Duration::from_millis(1000)));
        let mut reader = BufReader::new(&self.stream);
        let mut result = String::new();
        reader.read_line(&mut result)?;
        println!("Read {:?}", result);
        Ok(result)
    }
    pub fn write(&self, data: &str) -> Result<(), std::io::Error> {
        println!("Write: {}", data);
        let mut writer = BufWriter::new(&self.stream);
        writer.write_all(data.as_bytes())?;
        Ok(())
    }
}
