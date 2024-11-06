
mod log;
mod handler;
mod dto;

use actix_web::{App, web, HttpServer, main};
use actix_web::middleware::Logger;
use env_logger::Env;
use crate::log::Log;



#[main]
async fn main() -> std::io::Result<()>{
    let log = web::Data::new(Log::new());

    env_logger::init_from_env(Env::default().default_filter_or("info"));


    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .app_data(log.clone())
            .route("/append", web::post().to(handler::append))
            .route("/read", web::get().to(handler::read))

    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await

}
