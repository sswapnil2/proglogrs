use actix_web::{web, HttpResponse, Responder};
use actix_web::http::StatusCode;
use serde::{Deserialize, Serialize};
use crate::log::Log;

#[derive(Deserialize)]
pub struct AppendRequest {
    data: String
}

pub async fn append(log: web::Data<Log>, item: web::Json<AppendRequest>) -> impl Responder {
    let offset = log.append(item.data.as_bytes().to_vec());
    HttpResponse::Ok()
        .status(StatusCode::OK)
        .json(offset)

}

#[derive(Deserialize)]
pub struct ReadRequest {
    offset: u64
}

#[derive(Serialize)]
pub struct ReadResponse {
    offset: u64,
    value: String
}


pub async fn read(log: web::Data<Log>, item: web::Query<ReadRequest>) -> impl Responder {
    match log.read(item.offset) {
        Some(record) => HttpResponse::Ok().json(
            ReadResponse {
                offset: record.offset,
                value: String::from_utf8(record.value).expect("Error in conversion")
            }
        ),
        None => HttpResponse::NotFound()
            .body("Record Not Found")
    }

}