mod bindings {
    wit_bindgen::generate!({
        generate_all,
    });
}
use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::client;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response, Scheme};
use wit_bindgen::StreamResult;

struct Component;

impl Handler for Component {
    async fn handle(incoming_req: Request) -> Result<Response, ErrorCode> {
        // 1. Build outgoing request to the next component (backend)
        let headers = Fields::new(); // you can copy headers from incoming_req if needed
        println!("{:?}", headers);
        // headers
        //     .set("content-type", &[b"text/plain; charset=utf-8".to_vec()])
        //     .map_err(|_| ErrorCode::InternalError(None))?;

        let contents = None; // we'll forward the body later

        // Create a future for trailers (no trailers → Ok(None))
        // pub fn new<T>(default: fn() -> T) 
        // -> (wit_bindgen::rt::async_support::FutureWriter<T>, wit_bindgen::rt::async_support::FutureReader<T>)
        let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| todo!());
        let options = None; // no special options

        //pub fn new(headers: Headers, contents: Option<wit_bindgen::rt::async_support::StreamReader<u8>>
        // , trailers: wit_bindgen::rt::async_support::FutureReader<Result<Option<Trailers>, ErrorCode>>
        // , options: Option<RequestOptions>) 
        // -> (Request, wit_bindgen::rt::async_support::FutureReader<Result<(), ErrorCode>>)
        let (outgoing_req, req_raw_future_rd) = Request::new(headers, contents, trailers_rx, options);

        // Copy method, path, scheme, authority from incoming request
        outgoing_req
            .set_method(&incoming_req.get_method())
            .map_err(|_| ErrorCode::InternalError(None))?;

        let path_with_query = incoming_req
            .get_path_with_query()
            .unwrap_or_else(|| "/".to_string());
        outgoing_req
            .set_path_with_query(Some(&path_with_query))
            .map_err(|_| ErrorCode::InternalError(None))?;

        outgoing_req
            .set_scheme(Some(&Scheme::Http))
            .map_err(|_| ErrorCode::InternalError(None))?;

        outgoing_req
            .set_authority(Some("127.0.0.1:8002")) // address of backend
            .map_err(|_| ErrorCode::InternalError(None))?;

        // Optional: forward request body (if any)
        // (For simplicity, we skip forwarding body here – add later if needed)

        // 2. Send the request to the downstream component
        let downstream_response = client::send(outgoing_req)
            .await
            .map_err(|_| ErrorCode::InternalError(None))?;
        
        // Get the status code from downstream response
        let headers = downstream_response.get_headers();
        
        let hlist = headers.copy_all();
        for (name, value) in hlist {
            println!("{}: {}", name, String::from_utf8_lossy(&value).to_string());
        }

        // Consume the body of the downstream response (requires an abort future)

        //pub fn consume_body(this: Response, res: wit_bindgen::rt::async_support::FutureReader<Result<(), ErrorCode>>) 
        //-> (wit_bindgen::rt::async_support::StreamReader<u8>
        // , wit_bindgen::rt::async_support::FutureReader<Result<Option<Trailers>, ErrorCode>>)
        
        let (body_stream, _trailers_future) =
            bindings::wasi::http::types::Response::consume_body(downstream_response, req_raw_future_rd);
        
        println!("body_stream: {:?}", body_stream);

        // Create a new stream for the response we will return to the caller
        let (mut tx, rx) = bindings::wit_stream::new();
        let (resp_trailers_tx, resp_trailers_rx) = bindings::wit_future::new(|| todo!());

        // Spawn a task to forward body chunks from downstream to the caller
        wit_bindgen::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut body_stream = body_stream;
            loop {
                let (result, new_buf) = body_stream.read(buf).await;
                eprintln!("Read {} bytes, result: {:?}", new_buf.len(), result);
                buf = new_buf;

                println!("{:?}", Box::new(result));
                
                match result {
                    StreamResult::Cancelled => break,             // EOF
                    StreamResult::Complete(n) if n == 0 => break, // EOF
                    StreamResult::Complete(n) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            println!("Chunk: {}", text);
                        }
                        // n bytes read
                        let chunk = buf[..n].to_vec();
                        tx.write_all(chunk).await;
                        // continue with the returned buffer (buf is moved back)
                    }
                    StreamResult::Dropped => {
                        eprintln!("Stream error");
                        break;
                    }
                }
            }
            //let _ = resp_trailers_tx.write(Ok(None)).await;

            drop(tx);
            let _ = trailers_tx.write(Ok(None)).await;
            let _ = resp_trailers_tx.write(Ok(None)).await;
        });

        // Create the final response to return
        let (final_response, _send_future) =
            Response::new(headers, Some(rx), resp_trailers_rx);
        
        Ok(final_response)
    }

}

bindings::export!(Component with_types_in bindings);
