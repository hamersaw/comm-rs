#[macro_use]
extern crate log;

use std::error::Error;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

pub trait StreamHandler where Self: Send + Sync {
    fn process(&self, stream: &mut TcpStream) -> Result<(), Box<dyn Error>>;
}

pub struct Server<T: 'static + StreamHandler + Send + Sync> {
    join_handles: Vec<JoinHandle<()>>,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    sleep_ms: u64,
    stream_handler: Arc<T>,
}

impl<T: 'static + StreamHandler + Send + Sync> Server<T> {
    pub fn new(listener: TcpListener, sleep_ms: u64,
            stream_handler: Arc<T>) -> Server<T> {
        info!("initailizing server [local_address={:?}, sleep_ms={}]",
            listener.local_addr(), sleep_ms);
        Server {
            listener,
            sleep_ms,
            shutdown: Arc::new(AtomicBool::new(true)),
            stream_handler,
            join_handles: Vec::new(),
        }
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        info!("starting server");

        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // clone variables
        let listener_clone = self.listener.try_clone()?;
        listener_clone.set_nonblocking(true)?;
        let shutdown_clone = self.shutdown.clone();
        let sleep_duration = Duration::from_millis(self.sleep_ms);
        let stream_handler_clone = self.stream_handler.clone();

        // start thread to accept connections on TcpListener
        let join_handle = std::thread::spawn(move || {
            for result in listener_clone.incoming() {
                match result {
                    Ok(mut stream) => {
                        let stream_handler = stream_handler_clone.clone();
                        std::thread::spawn(move || {
                            // process stream
                            if let Err(e) = stream_handler
                                    .process(&mut stream) {
                                warn!("handler error: {}", e);
                            }
                        });
                    },
                    Err(ref e) if e.kind() ==
                            std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(sleep_duration);
                    },
                    Err(ref e) if e.kind() !=
                            std::io::ErrorKind::WouldBlock => {
                        error!("failed to connect client: {}", e);
                    },
                    _ => {},
                }

                // check if shutdown
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        });

        self.join_handles.push(join_handle);
        Ok(())
    }

    pub fn start_threadpool(&mut self,
            thread_count: u8) -> std::io::Result<()> {
        info!("starting threadpool [thread_count={}]", thread_count);

        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // start worker threads
        for _ in 0..thread_count {
            // clone variables
            let listener_clone = self.listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let shutdown_clone = self.shutdown.clone();
            let sleep_duration = Duration::from_millis(self.sleep_ms);
            let stream_handler_clone = self.stream_handler.clone();

            let join_handle = std::thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // process stream
                            if let Err(e) = stream_handler_clone
                                    .process(&mut stream) {
                                warn!("handler error: {}", e);
                            }
                        },
                        Err(ref e) if e.kind() ==
                                std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(sleep_duration);
                        },
                        Err(ref e) if e.kind() !=
                                std::io::ErrorKind::WouldBlock => {
                            error!("failed to connect client: {}", e);
                        },
                        _ => {},
                    }

                    // check if shutdown
                    if shutdown_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
            });

            self.join_handles.push(join_handle);
        }

        Ok(())
    }

    pub fn stop(mut self) -> std::thread::Result<()> {
        info!("stopping");
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // join threads
        while !self.join_handles.is_empty() {
            let join_handle = self.join_handles.pop().unwrap();
            join_handle.join()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Duration;

    use super::{Server, StreamHandler};

    #[test]
    fn cycle_server() {
        let address = "127.0.0.1:18000";

        struct NullHandler { }
        impl StreamHandler for NullHandler {
            fn process(&self, stream: &mut TcpStream)
                    -> Result<(), Box<dyn Error>> {
                let mut buf = vec!(0u8; 5);
                stream.read_exact(&mut buf).expect("stream read exact");

                let data = String::from_utf8(buf)
                    .expect("string decode");

                let data_reference: &str = &data;
                match data_reference {
                    "hello" => Ok(()),
                    _ => Err("test".into()),
                }
            }
        }

        // open server
        let listener = TcpListener::bind(&address)
            .expect("TcpListener bind");
        let stream_handler = Arc::new(NullHandler{ });
        let mut server = Server::new(listener, 50, stream_handler);

        // start server
        server.start().expect("server start");

        // valid connection
        {
            let mut stream = TcpStream::connect(&address)
                .expect("tcp stream connect");

            stream.write_all(b"hello").expect("stream write all");
        }

        // invalid connection
        {
            let mut stream = TcpStream::connect(&address)
                .expect("tcp stream connect");

            stream.write_all(b"world").expect("stream write all");
        }

        // sleep for 1 second
        let sleep_duration = Duration::from_millis(1000);
        std::thread::sleep(sleep_duration);

        // stop server
        server.stop().expect("server stop");
    }

    #[test]
    fn cycle_threadpool_server() {
        let address = "127.0.0.1:18001";

        struct NullHandler { }
        impl StreamHandler for NullHandler {
            fn process(&self, stream: &mut TcpStream)
                    -> Result<(), Box<dyn Error>> {
                let mut buf = vec!(0u8; 5);
                stream.read_exact(&mut buf).expect("stream read exact");

                let data = String::from_utf8(buf)
                    .expect("string decode");

                let data_reference: &str = &data;
                match data_reference {
                    "hello" => Ok(()),
                    _ => Err("test".into()),
                }
            }
        }

        // open server
        let listener = TcpListener::bind(&address)
            .expect("TcpListener bind");
        let stream_handler = Arc::new(NullHandler{ });
        let mut server = Server::new(listener, 50, stream_handler);

        // start server
        server.start_threadpool(2).expect("server start");

        // valid connection
        {
            let mut stream = TcpStream::connect(&address)
                .expect("tcp stream connect");

            stream.write_all(b"hello").expect("stream write all");
        }

        // invalid connection
        {
            let mut stream = TcpStream::connect(&address)
                .expect("tcp stream connect");

            stream.write_all(b"world").expect("stream write all");
        }

        // sleep for 1 second
        let sleep_duration = Duration::from_millis(1000);
        std::thread::sleep(sleep_duration);

        // stop server
        server.stop().expect("server stop");
    }
}
