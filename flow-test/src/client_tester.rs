use std::net::SocketAddr;

use bstr::ByteSlice;
use imap_flow::{
    client::{
        ClientFlow, ClientFlowCommandHandle, ClientFlowError, ClientFlowEvent, ClientFlowOptions,
    },
    stream::AnyStream,
};
use imap_types::{bounded_static::ToBoundedStatic, command::Command};
use tokio::net::TcpStream;
use tracing::trace;

use crate::codecs::Codecs;

/// A wrapper for `ClientFlow` suitable for testing.
pub struct ClientTester {
    codecs: Codecs,
    client_flow_options: ClientFlowOptions,
    connection_state: ConnectionState,
}

impl ClientTester {
    pub async fn new(
        codecs: Codecs,
        client_flow_options: ClientFlowOptions,
        server_address: SocketAddr,
    ) -> Self {
        let stream = TcpStream::connect(server_address).await.unwrap();
        trace!(?server_address, "Client is connected");
        Self {
            codecs,
            client_flow_options,
            connection_state: ConnectionState::Connected { stream },
        }
    }

    pub async fn receive_greeting(&mut self, expected_bytes: &[u8]) {
        let expected_greeting = self.codecs.decode_greeting(expected_bytes);
        match self.connection_state.take() {
            ConnectionState::Connected { stream } => {
                let stream = AnyStream::new(stream);
                let (client, greeting) =
                    ClientFlow::receive_greeting(stream, self.client_flow_options.clone())
                        .await
                        .unwrap();
                assert_eq!(expected_greeting, greeting);
                self.connection_state = ConnectionState::Greeted { client };
            }
            ConnectionState::Greeted { .. } => {
                panic!("Client is already greeted");
            }
            ConnectionState::Disconnected => {
                panic!("Client is already disconnected");
            }
        }
    }

    pub fn enqueue_command(&mut self, bytes: &[u8]) -> EnqueuedCommand {
        let command = self.codecs.decode_command_normalized(bytes).to_static();
        let client = self.connection_state.greeted();
        let handle = client.enqueue_command(command.to_static());
        EnqueuedCommand { command, handle }
    }

    pub async fn progress_command(&mut self, enqueued_command: EnqueuedCommand) {
        let client = self.connection_state.greeted();
        let event = client.progress().await.unwrap();
        match event {
            ClientFlowEvent::CommandSent { handle, command } => {
                assert_eq!(enqueued_command.handle, handle);
                assert_eq!(enqueued_command.command, command);
            }
            event => {
                panic!("Client emitted unexpected event: {event:?}");
            }
        }
    }

    pub async fn progress_rejected_command(
        &mut self,
        enqueued_command: EnqueuedCommand,
        status_bytes: &[u8],
    ) {
        let expected_status = self.codecs.decode_status(status_bytes);
        let client = self.connection_state.greeted();
        let event = client.progress().await.unwrap();
        match event {
            ClientFlowEvent::CommandRejected {
                handle,
                command,
                status,
            } => {
                assert_eq!(enqueued_command.handle, handle);
                assert_eq!(enqueued_command.command, command);
                assert_eq!(expected_status, status);
            }
            event => {
                panic!("Client emitted unexpected event: {event:?}");
            }
        }
    }

    pub async fn send_command(&mut self, bytes: &[u8]) {
        let enqueued_command = self.enqueue_command(bytes);
        self.progress_command(enqueued_command).await;
    }

    pub async fn send_rejected_command(&mut self, command_bytes: &[u8], status_bytes: &[u8]) {
        let enqueued_command = self.enqueue_command(command_bytes);
        self.progress_rejected_command(enqueued_command, status_bytes)
            .await;
    }

    pub async fn receive_data(&mut self, expected_bytes: &[u8]) {
        let expected_data = self.codecs.decode_data(expected_bytes);
        let client = self.connection_state.greeted();
        match client.progress().await.unwrap() {
            ClientFlowEvent::DataReceived { data } => {
                assert_eq!(expected_data, data);
            }
            event => {
                panic!("Client emitted unexpected event: {event:?}");
            }
        }
    }

    pub async fn receive_status(&mut self, expected_bytes: &[u8]) {
        let expected_status = self.codecs.decode_status(expected_bytes);
        let client = self.connection_state.greeted();
        match client.progress().await.unwrap() {
            ClientFlowEvent::StatusReceived { status } => {
                assert_eq!(expected_status, status);
            }
            event => {
                panic!("Client emitted unexpected event: {event:?}");
            }
        }
    }

    pub async fn receive_error_because_malformed_message(&mut self, expected_bytes: &[u8]) {
        let error = match self.connection_state.take() {
            ConnectionState::Connected { stream } => {
                let stream = AnyStream::new(stream);
                ClientFlow::receive_greeting(stream, self.client_flow_options.clone())
                    .await
                    .unwrap_err()
            }
            ConnectionState::Greeted { mut client } => {
                let error = client.progress().await.unwrap_err();
                self.connection_state = ConnectionState::Greeted { client };
                error
            }
            ConnectionState::Disconnected => {
                panic!("Client is already disconnected")
            }
        };
        match error {
            ClientFlowError::MalformedMessage { discarded_bytes } => {
                assert_eq!(expected_bytes.as_bstr(), discarded_bytes.as_bstr());
            }
            error => {
                panic!("Client emitted unexpected error: {error:?}");
            }
        }
    }
}

/// The current state of the connection between client and server.
#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    /// The client has established a TCP connection to the server.
    Connected { stream: TcpStream },
    /// The client was greeted by the server.
    Greeted { client: ClientFlow },
    /// The TCP connection between client and server was dropped.
    Disconnected,
}

impl ConnectionState {
    /// Assumes that the client was already greeted by the server and returns the `ClientFlow`.
    fn greeted(&mut self) -> &mut ClientFlow {
        match self {
            ConnectionState::Connected { .. } => {
                panic!("Client is not greeted yet");
            }
            ConnectionState::Greeted { client } => client,
            ConnectionState::Disconnected => {
                panic!("Client is already disconnected");
            }
        }
    }

    fn take(&mut self) -> ConnectionState {
        std::mem::replace(self, ConnectionState::Disconnected)
    }
}

/// A command that was enqueued and can later be used for assertions.
pub struct EnqueuedCommand {
    handle: ClientFlowCommandHandle,
    command: Command<'static>,
}
