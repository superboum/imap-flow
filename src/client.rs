use std::fmt::Debug;

use bytes::BytesMut;
use imap_codec::{
    decode::{GreetingDecodeError, ResponseDecodeError},
    AuthenticateDataCodec, CommandCodec, GreetingCodec, IdleDoneCodec, ResponseCodec,
};
use imap_types::{
    auth::AuthenticateData,
    command::Command,
    response::{CommandContinuationRequest, Data, Greeting, Response, Status},
};
use thiserror::Error;

use crate::{
    handle::{Handle, HandleGenerator, HandleGeneratorGenerator, RawHandle},
    receive::{ReceiveEvent, ReceiveState},
    send_command::{SendCommandEvent, SendCommandState, SendCommandTermination},
    stream::{AnyStream, StreamError},
    types::CommandAuthenticate,
};

static HANDLE_GENERATOR_GENERATOR: HandleGeneratorGenerator<ClientFlowCommandHandle> =
    HandleGeneratorGenerator::new();

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ClientFlowOptions {
    pub crlf_relaxed: bool,
}

impl Default for ClientFlowOptions {
    fn default() -> Self {
        Self {
            // Lean towards usability
            crlf_relaxed: true,
        }
    }
}

#[derive(Debug)]
pub struct ClientFlow {
    stream: AnyStream,

    handle_generator: HandleGenerator<ClientFlowCommandHandle>,
    send_command_state: SendCommandState,
    receive_response_state: ReceiveState<ResponseCodec>,
}

impl ClientFlow {
    pub async fn receive_greeting(
        mut stream: AnyStream,
        options: ClientFlowOptions,
    ) -> Result<(Self, Greeting<'static>), ClientFlowError> {
        // Receive greeting.
        let mut receive_greeting_state = ReceiveState::new(
            GreetingCodec::default(),
            options.crlf_relaxed,
            BytesMut::new(),
        );

        let greeting = match receive_greeting_state.progress(&mut stream).await? {
            ReceiveEvent::DecodingSuccess(greeting) => {
                receive_greeting_state.finish_message();
                greeting
            }
            ReceiveEvent::DecodingFailure(
                GreetingDecodeError::Failed | GreetingDecodeError::Incomplete,
            ) => {
                let discarded_bytes = receive_greeting_state.discard_message();
                return Err(ClientFlowError::MalformedMessage { discarded_bytes });
            }
            ReceiveEvent::ExpectedCrlfGotLf => {
                let discarded_bytes = receive_greeting_state.discard_message();
                return Err(ClientFlowError::ExpectedCrlfGotLf { discarded_bytes });
            }
        };

        // Create state to send commands ...
        let send_command_state = SendCommandState::new(
            CommandCodec::default(),
            AuthenticateDataCodec::default(),
            IdleDoneCodec::default(),
            BytesMut::new(),
        );

        // ..., and state to receive responses.
        let receive_response_state = receive_greeting_state.change_codec(ResponseCodec::new());

        let client_flow = Self {
            stream,
            handle_generator: HANDLE_GENERATOR_GENERATOR.generate(),
            send_command_state,
            receive_response_state,
        };

        Ok((client_flow, greeting))
    }

    /// Enqueues the [`Command`] for being sent to the client.
    ///
    /// The [`Command`] is not sent immediately but during one of the next calls of
    /// [`ClientFlow::progress`]. All [`Command`]s are sent in the same order they have been
    /// enqueued.
    pub fn enqueue_command(&mut self, command: Command<'static>) -> ClientFlowCommandHandle {
        let handle = self.handle_generator.generate();
        self.send_command_state.enqueue(handle, command);
        handle
    }

    pub async fn progress(&mut self) -> Result<ClientFlowEvent, ClientFlowError> {
        // The client must do two things:
        // - Sending commands to the server.
        // - Receiving responses from the server.
        //
        // There are two ways to accomplish that:
        // - Doing both in parallel.
        // - Doing both consecutively.
        //
        // Doing both in parallel is more complicated because both operations share the same
        // state and the borrow checker prevents the naive approach. We would need to introduce
        // interior mutability.
        //
        // Doing both consecutively is easier. But in which order? Receiving responses will block
        // indefinitely because we never know when the server is sending the next response.
        // Sending commands will be completed in the foreseeable future because for practical
        // purposes we can assume that the number of commands is finite and the stream will be
        // able to transfer all bytes soon.
        //
        // Therefore we prefer the second approach and begin with sending the commands.
        loop {
            if let Some(event) = self.progress_send().await? {
                return Ok(event);
            }

            if let Some(event) = self.progress_receive().await? {
                return Ok(event);
            }
        }
    }

    async fn progress_send(&mut self) -> Result<Option<ClientFlowEvent>, ClientFlowError> {
        match self.send_command_state.progress(&mut self.stream).await? {
            Some(SendCommandEvent::Command { handle, command }) => {
                Ok(Some(ClientFlowEvent::CommandSent { handle, command }))
            }
            Some(SendCommandEvent::CommandAuthenticate { handle }) => {
                Ok(Some(ClientFlowEvent::AuthenticateStarted { handle }))
            }
            Some(SendCommandEvent::CommandIdle { handle }) => {
                Ok(Some(ClientFlowEvent::IdleCommandSent { handle }))
            }
            Some(SendCommandEvent::IdleDone { handle }) => {
                Ok(Some(ClientFlowEvent::IdleDoneSent { handle }))
            }
            None => Ok(None),
        }
    }

    async fn progress_receive(&mut self) -> Result<Option<ClientFlowEvent>, ClientFlowError> {
        let event = loop {
            let response = match self
                .receive_response_state
                .progress(&mut self.stream)
                .await?
            {
                ReceiveEvent::DecodingSuccess(response) => {
                    self.receive_response_state.finish_message();
                    response
                }
                ReceiveEvent::DecodingFailure(ResponseDecodeError::LiteralFound { length }) => {
                    // The client must accept the literal in any case.
                    self.receive_response_state.start_literal(length);
                    continue;
                }
                ReceiveEvent::DecodingFailure(
                    ResponseDecodeError::Failed | ResponseDecodeError::Incomplete,
                ) => {
                    let discarded_bytes = self.receive_response_state.discard_message();
                    return Err(ClientFlowError::MalformedMessage { discarded_bytes });
                }
                ReceiveEvent::ExpectedCrlfGotLf => {
                    let discarded_bytes = self.receive_response_state.discard_message();
                    return Err(ClientFlowError::ExpectedCrlfGotLf { discarded_bytes });
                }
            };

            match response {
                Response::Status(status) => {
                    let event = if let Some(finish_result) =
                        self.send_command_state.maybe_remove(&status)
                    {
                        match finish_result {
                            SendCommandTermination::LiteralRejected { handle, command } => {
                                ClientFlowEvent::CommandRejected {
                                    handle,
                                    command,
                                    status,
                                }
                            }
                            SendCommandTermination::AuthenticateAccepted {
                                handle,
                                command_authenticate,
                            } => ClientFlowEvent::AuthenticateAccepted {
                                handle,
                                command_authenticate,
                                status,
                            },
                            SendCommandTermination::AuthenticateRejected {
                                handle,
                                command_authenticate,
                            } => ClientFlowEvent::AuthenticateRejected {
                                handle,
                                command_authenticate,
                                status,
                            },
                            SendCommandTermination::IdleRejected { handle } => {
                                ClientFlowEvent::IdleRejected { handle, status }
                            }
                        }
                    } else {
                        ClientFlowEvent::StatusReceived { status }
                    };

                    break Some(event);
                }
                Response::Data(data) => break Some(ClientFlowEvent::DataReceived { data }),
                Response::CommandContinuationRequest(continuation) => {
                    if self.send_command_state.literal_continue() {
                        // We received a continuation that was necessary for sending a command.
                        // So we abort receiving responses for now and continue with sending commands.
                        break None;
                    } else if let Some(handle) = self.send_command_state.authenticate_continue() {
                        break Some(ClientFlowEvent::ContinuationAuthenticateReceived {
                            handle,
                            continuation,
                        });
                    } else if let Some(handle) = self.send_command_state.idle_continue() {
                        break Some(ClientFlowEvent::IdleAccepted {
                            handle,
                            continuation,
                        });
                    } else {
                        break Some(ClientFlowEvent::ContinuationReceived { continuation });
                    }
                }
            }
        };

        Ok(event)
    }

    pub fn set_authenticate_data(
        &mut self,
        authenticate_data: AuthenticateData,
    ) -> Result<ClientFlowCommandHandle, AuthenticateData> {
        self.send_command_state
            .set_authenticate_data(authenticate_data)
    }

    pub fn set_idle_done(&mut self) -> Option<ClientFlowCommandHandle> {
        self.send_command_state.set_idle_done()
    }

    #[cfg(feature = "expose_stream")]
    /// Return the underlying stream for debug purposes (or experiments).
    ///
    /// Note: Writing to or reading from the stream may introduce
    /// conflicts with imap-flow.
    pub fn stream_mut(&mut self) -> &mut AnyStream {
        &mut self.stream
    }
}

/// A handle for an enqueued [`Command`].
///
/// This handle can be used to track the sending progress. After a [`Command`] was enqueued via
/// [`ClientFlow::enqueue_command`] it is in the process of being sent until
/// [`ClientFlow::progress`] returns a [`ClientFlowEvent::CommandSent`] or
/// [`ClientFlowEvent::CommandRejected`] with the corresponding handle.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct ClientFlowCommandHandle(RawHandle);

impl Handle for ClientFlowCommandHandle {
    fn from_raw(handle: RawHandle) -> Self {
        Self(handle)
    }
}

// Implement a short debug representation that hides the underlying raw handle
impl Debug for ClientFlowCommandHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientFlowCommandHandle")
            .field(&self.0.generator_id())
            .field(&self.0.handle_id())
            .finish()
    }
}

#[derive(Debug)]
pub enum ClientFlowEvent {
    /// Enqueued [`Command`] successfully sent.
    CommandSent {
        /// Handle to the enqueued [`Command`].
        handle: ClientFlowCommandHandle,
        /// Formerly enqueued [`Command`].
        command: Command<'static>,
    },
    /// Enqueued [`Command`] rejected.
    ///
    /// Note: Emitted when the server rejected a command literal with `BAD`.
    CommandRejected {
        /// Handle to the enqueued [`Command`].
        handle: ClientFlowCommandHandle,
        /// Formerly enqueued [`Command`].
        command: Command<'static>,
        /// [`Status`] sent by the server in order to reject the [`Command`].
        ///
        /// Note: [`ClientFlow`] already handled this [`Status`] but it might still have
        /// useful information that could be logged or displayed to the user
        /// (e.g. [`Code::Alert`](imap_types::response::Code::Alert)).
        status: Status<'static>,
    },
    AuthenticateStarted {
        handle: ClientFlowCommandHandle,
    },
    /// Server is requesting (more) authentication data.
    ///
    /// The client MUST call [`ClientFlow::authenticate_continue`] next.
    ///
    /// Note: The client can also progress the authentication by sending [`AuthenticateData::Cancel`].
    /// However, it's up to the server to abort the authentication flow by sending a tagged status
    /// response. In this case, the client will receive either a [`ClientFlowEvent::AuthenticateAccepted`]
    /// or [`ClientFlowEvent::AuthenticateRejected`] event.
    ContinuationAuthenticateReceived {
        /// Handle to the enqueued [`Command`].
        handle: ClientFlowCommandHandle,
        continuation: CommandContinuationRequest<'static>,
    },
    AuthenticateAccepted {
        handle: ClientFlowCommandHandle,
        command_authenticate: CommandAuthenticate,
        status: Status<'static>,
    },
    AuthenticateRejected {
        handle: ClientFlowCommandHandle,
        command_authenticate: CommandAuthenticate,
        status: Status<'static>,
    },
    IdleCommandSent {
        handle: ClientFlowCommandHandle,
    },
    IdleAccepted {
        handle: ClientFlowCommandHandle,
        continuation: CommandContinuationRequest<'static>,
    },
    IdleRejected {
        handle: ClientFlowCommandHandle,
        status: Status<'static>,
    },
    IdleDoneSent {
        handle: ClientFlowCommandHandle,
    },
    /// Server [`Data`] received.
    DataReceived {
        data: Data<'static>,
    },
    /// Server [`Status`] received.
    StatusReceived {
        status: Status<'static>,
    },
    /// Server [`CommandContinuationRequest`] response received.
    ///
    /// Note: The received continuation was not part of [`ClientFlow`] handling.
    ContinuationReceived {
        continuation: CommandContinuationRequest<'static>,
    },
}

#[derive(Debug, Error)]
pub enum ClientFlowError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error("Expected `\\r\\n`, got `\\n`")]
    ExpectedCrlfGotLf { discarded_bytes: Box<[u8]> },
    #[error("Received malformed message")]
    MalformedMessage { discarded_bytes: Box<[u8]> },
}
