mod bindings {
    wit_bindgen::generate!({
        generate_all,
    });
}
use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use oxiwhisper::{TranscribeOptions, WhisperModel};
use std::path::Path;

struct Component;

impl Handler for Component {
    async fn handle(_request: Request) -> Result<Response, ErrorCode> {
        let headers = Fields::new();

        if let Err(_) = headers.set("content-type", &[b"text/plain; charset=utf-8".to_vec()]) {
            return Err(ErrorCode::InternalError(None));
        }

        let (mut tx, rx) = bindings::wit_stream::new();
        let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| todo!());

        wit_bindgen::spawn(async move {
            let model = WhisperModel::from_file(Path::new("data/ggml-tiny.bin")).unwrap();
            let audio = oxiwhisper::audio::load_wav(Path::new("data/output.wav")).unwrap();
            //let text = model.transcribe(&audio, &TranscribeOptions::default()).unwrap();

            let opts = TranscribeOptions {
                timestamps: true,
                ..TranscribeOptions::default()
            };

            let mut stream = model.stream(opts);
            stream.push_audio(&audio);

            // 4. Retrieve segments (each contains timestamps)
            while let Some(result) = stream.next_segment() {
                match result {
                    Ok(segment) => {
                        let seg = format!(
                            "[{:.2}s -> {:.2}s]: {}",
                            segment.start, // Start time in seconds
                            segment.end,   // End time in seconds
                            segment.text
                        );

                        println!("{}", seg);

                        tx.write_all(seg.into_bytes()).await;

                        // if segment.end > 100.0 {
                        //     break;
                        // }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            }

            drop(tx);
            let _ = trailers_tx.write(Ok(None)).await;
        });

        let (response, _result) = Response::new(headers, Some(rx), trailers_rx);

        Ok(response)
    }
}

bindings::export!(Component with_types_in bindings);
