mod error;
mod ticket;

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::stream::{self, Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::core::MurrError;
use crate::service::MurrService;

use ticket::FetchTicket;

pub struct MurrFlightService {
    service: Arc<MurrService>,
}

impl MurrFlightService {
    pub fn new(service: Arc<MurrService>) -> Self {
        Self { service }
    }

    pub async fn serve(self, addr: &str) -> Result<(), MurrError> {
        let addr = addr
            .parse()
            .map_err(|e| MurrError::ConfigParsingError(format!("invalid address: {e}")))?;

        Server::builder()
            .add_service(FlightServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| MurrError::IoError(format!("Flight server error: {e}")))?;

        Ok(())
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl FlightService for MurrFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let fetch: FetchTicket = serde_json::from_slice(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("invalid ticket JSON: {e}")))?;

        let keys: Vec<&str> = fetch.keys.iter().map(|s| s.as_str()).collect();
        let columns: Vec<&str> = fetch.columns.iter().map(|s| s.as_str()).collect();

        let batch = self
            .service
            .read(&fetch.table, &keys, &columns)
            .await
            .map_err(Status::from)?;

        let stream = FlightDataEncoderBuilder::new()
            .build(stream::once(async { Ok(batch) }))
            .map(|result| result.map_err(|e| e.into()));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let table_name = descriptor
            .path
            .first()
            .ok_or_else(|| Status::invalid_argument("path must contain table name"))?;

        let schema = self
            .service
            .get_schema(table_name)
            .await
            .map_err(Status::from)?;
        let arrow_schema: Schema = (&schema).into();

        let info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let table_name = descriptor
            .path
            .first()
            .ok_or_else(|| Status::invalid_argument("path must contain table name"))?;

        let schema = self
            .service
            .get_schema(table_name)
            .await
            .map_err(Status::from)?;
        let arrow_schema: Schema = (&schema).into();
        let options = IpcWriteOptions::default();

        let result = SchemaResult::try_from(SchemaAsIpc::new(&arrow_schema, &options))
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(result))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let tables = self.service.list_tables().await;
        let infos: Vec<Result<FlightInfo, Status>> = tables
            .into_iter()
            .map(|(name, schema)| {
                let arrow_schema: Schema = (&schema).into();
                let descriptor = FlightDescriptor::new_path(vec![name]);
                FlightInfo::new()
                    .try_with_schema(&arrow_schema)
                    .map(|info| info.with_descriptor(descriptor))
                    .map_err(|e| Status::internal(e.to_string()))
            })
            .collect();

        Ok(Response::new(Box::pin(stream::iter(infos))))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }
}
