use imap_codec::imap_types::response::{Greeting, Status, CommandContinuationRequest};
use imap_codec::imap_types::command::{Command, CommandBody};
use imap_flow::{
    server::{ServerFlow, ServerFlowEvent, ServerFlowOptions, ServerFlowResponseHandle},
    stream::AnyStream,
};
use tokio::net::TcpListener;
use bytes::BytesMut;

enum HandleT {
    None,
    Normal(ServerFlowResponseHandle),
    Idle(ServerFlowResponseHandle),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let stream = {
        let listener = TcpListener::bind("127.0.0.1:12345").await.unwrap();

        let (stream, _) = listener.accept().await.unwrap();

        stream
    };

    let (mut server, _) = ServerFlow::send_greeting(
        AnyStream::new(stream),
        ServerFlowOptions::default(),
        Greeting::ok(None, "Hello, World!").unwrap(),
    )
    .await
    .unwrap();

    let mut handle = HandleT::None;

    loop {
        match server.progress().await.unwrap() {
            ServerFlowEvent::CommandReceived { command } => {
                println!("command received: {command:?}");
                match command.body {
                    CommandBody::Idle => {
                        let cr =  CommandContinuationRequest::basic(None, "idling").unwrap();
                        let h = server.enqueue_continuation(cr);
                        handle = HandleT::Idle(h);
                    },
                    _ => {
                        handle = HandleT::Normal(
                            server.enqueue_status(Status::ok(Some(command.tag), None, "...").unwrap()),
                        );
                    }
                }
            }
            ServerFlowEvent::ResponseSent {
                handle: got_handle,
                response,
            } => {
                println!("response sent: {response:?}");
                match handle {
                    HandleT::Idle(h) if h == got_handle => {
                        println!("idle response sent with a continuation request");
                        let mut buf = BytesMut::with_capacity(10);
                        // @FIXME use a tokio::select here to write data
                        // at the same time we read user's inputs
                        server.stream.read(&mut buf).await.unwrap();
                        assert_eq!(&buf[..], &b"DONE\n"[..]);
                        println!("read success");
                    },
                    HandleT::Normal(h) if h == got_handle => {
                        println!("normal response sent");
                    },
                    _ => unreachable!(),
                }
            }
        }
    }
}
