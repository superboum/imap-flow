use std::{collections::VecDeque, fmt::Debug};

use bytes::BytesMut;
use imap_codec::encode::{Encoder, Fragment};

use crate::{
    server::ServerFlowResponseHandle,
    stream::{AnyStream, StreamError},
};

#[derive(Debug)]
pub struct SendResponseState<C: Encoder>
where
    C::Message<'static>: Debug,
{
    codec: C,
    // FIFO queue for responses that should be sent next.
    queued_responses: VecDeque<QueuedResponse<C>>,
    // The response that is currently being sent.
    current_response: Option<CurrentResponse<C>>,
    // Used for writing the current response to the stream.
    // Should be empty if `current_response` is `None`.
    write_buffer: BytesMut,
}

impl<C: Encoder> SendResponseState<C>
where
    C::Message<'static>: Debug,
{
    pub fn new(codec: C, write_buffer: BytesMut) -> Self {
        Self {
            codec,
            queued_responses: VecDeque::new(),
            current_response: None,
            write_buffer,
        }
    }

    pub fn enqueue(
        &mut self,
        handle: Option<ServerFlowResponseHandle>,
        response: C::Message<'static>,
    ) {
        self.queued_responses
            .push_back(QueuedResponse { handle, response });
    }

    pub fn finish(mut self) -> BytesMut {
        self.write_buffer.clear();
        self.write_buffer
    }

    pub async fn progress(
        &mut self,
        stream: &mut AnyStream,
    ) -> Result<Option<SendResponseEvent<C>>, StreamError> {
        let current_response = match self.current_response.take() {
            Some(current_response) => {
                // We are currently sending a response but the sending process was cancelled.
                // Continue the sending process.
                current_response
            }
            None => {
                assert!(self.write_buffer.is_empty());

                let Some(queued_response) = self.queued_responses.pop_front() else {
                    // There is currently no response that needs to be sent
                    return Ok(None);
                };

                queued_response.push_to_buffer(&mut self.write_buffer, &self.codec)
            }
        };

        // Store the current response to ensure cancellation safety
        self.current_response = Some(current_response);

        // Send all bytes of current response
        stream.write_all(&mut self.write_buffer).await?;

        // Restore the current response, can't fail because we set it to `Some` above
        let current_response = self.current_response.take().unwrap();

        // We finished sending a response completely
        Ok(Some(SendResponseEvent {
            handle: current_response.handle,
            response: current_response.response,
        }))
    }
}

/// A response that is queued but not sent yet.
#[derive(Debug)]
struct QueuedResponse<C: Encoder>
where
    C::Message<'static>: Debug,
{
    handle: Option<ServerFlowResponseHandle>,
    response: C::Message<'static>,
}

impl<C: Encoder> QueuedResponse<C>
where
    C::Message<'static>: Debug,
{
    fn push_to_buffer(self, write_buffer: &mut BytesMut, codec: &C) -> CurrentResponse<C> {
        for fragment in codec.encode(&self.response) {
            let data = match fragment {
                Fragment::Line { data } => data,
                // Note: The server doesn't need to wait before sending a literal.
                //       Thus, non-sync literals doesn't make sense here.
                //       This is currently an issue in imap-codec,
                //       see https://github.com/duesee/imap-codec/issues/332
                Fragment::Literal { data, .. } => data,
            };
            write_buffer.extend(data);
        }

        CurrentResponse {
            handle: self.handle,
            response: self.response,
        }
    }
}

/// A response that is currently being sent.
#[derive(Debug)]
struct CurrentResponse<C: Encoder>
where
    C::Message<'static>: Debug,
{
    handle: Option<ServerFlowResponseHandle>,
    response: C::Message<'static>,
}

/// A response was sent.
#[derive(Debug)]
pub struct SendResponseEvent<C: Encoder> {
    pub handle: Option<ServerFlowResponseHandle>,
    pub response: C::Message<'static>,
}
