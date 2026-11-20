use memo_map::MemoMap;
use reqwest::{RequestBuilder, StatusCode};
#[cfg(feature = "tracing")]
use tracing::{self as log, Instrument};

use super::{retry_after_delay, RateLimit, RateLimitType};
use crate::time::{sleep, Duration};
use crate::{ResponseInfo, RiotApiConfig, RiotApiError, TryRequestError, TryRequestResult};

pub struct RegionalRequester {
    /// The app rate limit.
    app_rate_limit: RateLimit,
    /// Method rate limits.
    method_rate_limits: MemoMap<&'static str, RateLimit>,
}

impl RegionalRequester {
    /// HTTP status codes which are considered a success but will results in `None`.
    const NONE_STATUS_CODES: [StatusCode; 2] = [
        StatusCode::NO_CONTENT, // 204
        StatusCode::NOT_FOUND,  // 404
    ];

    pub fn new() -> Self {
        Self {
            app_rate_limit: RateLimit::new(RateLimitType::Application),
            method_rate_limits: MemoMap::new(),
        }
    }

    pub async fn execute<'a>(
        &'a self,
        config: &'a RiotApiConfig,
        method_id: &'static str,
        request: RequestBuilder,
        min_capacity: Option<f32>,
    ) -> TryRequestResult<ResponseInfo> {
        let mut retries: u8 = 0;
        let mut reqwest_errors = Vec::new();
        loop {
            let method_rate_limit = self
                .method_rate_limits
                .get_or_insert(&method_id, || RateLimit::new(RateLimitType::Method));

            if !config.rate_limiting {
            } else if let Some(min_capacity) = min_capacity {
                // Never sleep, return `None` if we don't have enough capacity.
                if !RateLimit::acquire_both_if_above_capacity(
                    &self.app_rate_limit,
                    method_rate_limit,
                    min_capacity,
                ) {
                    return Err(TryRequestError::NotEnoughCapacity);
                }
            } else {
                // Sleep until we can aquire both.
                let rate_limit = RateLimit::acquire_both(&self.app_rate_limit, method_rate_limit);
                #[cfg(feature = "tracing")]
                let rate_limit = rate_limit.instrument(tracing::info_span!("rate_limit"));
                rate_limit.await;
            }

            // Send request.
            let request_clone = request
                .try_clone()
                .expect("Failed to clone request.")
                .send();
            #[cfg(feature = "tracing")]
            let request_clone = request_clone.instrument(tracing::info_span!("request"));
            let response = request_clone.await;
            let response = match response {
                Ok(response) => response,
                // Check for lower level errors, like connection errors.
                Err(e) => {
                    reqwest_errors.push(e);
                    if retries >= config.retries {
                        log::debug!(
                            "Request failed (retried {} times), failure, returning error.",
                            retries
                        );
                        break Err(TryRequestError::RiotApiError(RiotApiError::new(
                            reqwest_errors,
                            None,
                            retries,
                            None,
                            None,
                        )));
                    }
                    let delay = Duration::from_secs(2_u64.pow(retries as u32));
                    log::debug!(
                        "Request failed with cause \"{}\", (retried {} times), using exponential backoff, retrying after {:?}.",
                        reqwest_errors.last().unwrap().to_string(), retries, delay,
                    );
                    let backoff = sleep(delay);
                    #[cfg(feature = "tracing")]
                    let backoff = backoff.instrument(tracing::info_span!("backoff"));
                    backoff.await;
                    retries += 1;
                    continue;
                }
            };
            let status = response.status();
            let retry_after = if config.rate_limiting {
                // Maybe update rate limits (based on response headers).
                // Call both, with no short circuiting, so each limit sees the response.
                let retry_after_app = self.app_rate_limit.on_response(config, &response);
                let retry_after_method = method_rate_limit.on_response(config, &response);
                retry_after_app.or(retry_after_method) // Only the limit the 429 blames returns `Some`.
            } else if StatusCode::TOO_MANY_REQUESTS == status {
                Some(retry_after_delay(&response))
            } else {
                None
            };

            // Handle normal success / failure cases.
            let status_none = Self::NONE_STATUS_CODES.contains(&status);
            // Success case.
            if status.is_success() || status_none {
                log::trace!(
                    "Response {} (retried {} times), success, returning result.",
                    status,
                    retries
                );
                break Ok(ResponseInfo {
                    response,
                    retries,
                    status_none,
                    reqwest_errors,
                });
            }
            reqwest_errors.push(response.error_for_status_ref().err().unwrap_or_else(|| {
                panic!(
                    "Unhandlable response status code, neither success nor failure: {}.",
                    status
                )
            }));

            // Failure, may or may not be retryable.
            // Not-retryable: no more retries or 4xx or ? (3xx, redirects exceeded).
            // Retryable: retries remaining, and 429 or 5xx.
            if retries >= config.retries
                || (StatusCode::TOO_MANY_REQUESTS != status && !status.is_server_error())
            {
                log::debug!(
                    "Response {} (retried {} times), failure, returning error.",
                    status,
                    retries
                );
                break Err(TryRequestError::RiotApiError(RiotApiError::new(
                    reqwest_errors,
                    None,
                    retries,
                    Some(response),
                    Some(status),
                )));
            }

            // Is retryable, do exponential backoff if retry-after wasn't specified.
            // 1 sec, 2 sec, 4 sec, 8 sec.
            match retry_after {
                Some(delay) => {
                    log::debug!(
                        "Response {} (retried {} times), `retry-after` set, retrying after {:?}.",
                        status,
                        retries,
                        delay
                    );
                    // With rate limiting on, the next acquire waits out the delay.
                    // A `try_*` acquire returns `NotEnoughCapacity` instead.
                    if !config.rate_limiting {
                        let backoff = sleep(delay);
                        #[cfg(feature = "tracing")]
                        let backoff = backoff.instrument(tracing::info_span!("backoff"));
                        backoff.await;
                    }
                }
                None => {
                    let delay = Duration::from_secs(2 + 2_u64.pow(retries as u32));
                    log::debug!("Response {} (retried {} times), NO `retry-after`, using exponential backoff, retrying after {:?}.", status, retries, delay);
                    let backoff = sleep(delay);
                    #[cfg(feature = "tracing")]
                    let backoff = backoff.instrument(tracing::info_span!("backoff"));
                    backoff.await;
                }
            }
            retries += 1;
        }
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Response, Server};
    use parking_lot::Mutex;
    use reqwest::Method;

    use crate::time::{Duration, Instant};
    use crate::{RiotApi, RiotApiConfig, TryRequestError};

    const METHOD_ID: &str = "test-v1.getThing";
    const PLATFORM: &str = "na1";

    /// A local stand-in for the Riot API that answers with `responses` in
    /// order, then with a bare 200 once they run out.
    struct ScriptedRiotServer {
        responses: Mutex<VecDeque<Response<Body>>>,
        request_count: AtomicUsize,
    }

    impl ScriptedRiotServer {
        /// Starts the server and returns it with a `RiotApi` pointed at it.
        async fn start(
            responses: Vec<Response<Body>>,
            config: RiotApiConfig,
        ) -> (Arc<Self>, RiotApi) {
            let server = Arc::new(Self {
                responses: Mutex::new(responses.into()),
                request_count: AtomicUsize::new(0),
            });

            let handler = server.clone();
            let make_service = make_service_fn(move |_| {
                let handler = handler.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |_| {
                        let handler = handler.clone();
                        async move { Ok::<_, Infallible>(handler.respond()) }
                    }))
                }
            });
            let http_server = Server::bind(&([127, 0, 0, 1], 0).into()).serve(make_service);
            let base_url = format!("http://{}", http_server.local_addr());
            tokio::spawn(http_server);

            (server, RiotApi::new(config.set_base_url(base_url)))
        }

        fn respond(&self) -> Response<Body> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .pop_front()
                .unwrap_or_else(|| Response::new(Body::from("{}")))
        }

        fn request_count(&self) -> usize {
            self.request_count.load(Ordering::SeqCst)
        }
    }

    /// A Riot-side throttle 429 that also reports roomy rate limits, so any
    /// refusal after it comes from the 429 and not from a cold bucket.
    fn service_throttle(retry_after_secs: u32) -> Response<Body> {
        Response::builder()
            .status(429)
            .header("X-Rate-Limit-Type", "service")
            .header("Retry-After", retry_after_secs.to_string())
            .header("X-App-Rate-Limit", "100:1")
            .header("X-App-Rate-Limit-Count", "1:1")
            .header("X-Method-Rate-Limit", "100:1")
            .header("X-Method-Rate-Limit-Count", "1:1")
            .body(Body::empty())
            .unwrap()
    }

    fn rate_limited_config() -> RiotApiConfig {
        RiotApiConfig::with_key("RGAPI-test").set_retries(0)
    }

    fn unlimited_config() -> RiotApiConfig {
        rate_limited_config().set_rate_limiting(false)
    }

    /// Sends one `try_*` request and returns the status Riot answered with, or
    /// `None` when Riven refused to send it.
    async fn try_send(api: &RiotApi) -> Option<u16> {
        let request = api.request(Method::GET, PLATFORM, "/");
        match api.try_execute_raw(METHOD_ID, PLATFORM, request, 0.0).await {
            Ok(info) => Some(info.response.status().as_u16()),
            Err(TryRequestError::RiotApiError(error)) => Some(
                error
                    .status_code()
                    .expect("the test server always responds")
                    .as_u16(),
            ),
            Err(TryRequestError::NotEnoughCapacity) => None,
        }
    }

    #[tokio::test]
    async fn rate_limiting_on_refuses_while_a_throttle_429_is_parked() {
        let (server, api) =
            ScriptedRiotServer::start(vec![service_throttle(1)], rate_limited_config()).await;

        assert_eq!(Some(429), try_send(&api).await);
        assert_eq!(None, try_send(&api).await);
        assert_eq!(1, server.request_count());
    }

    #[tokio::test]
    async fn rate_limiting_off_sends_through_a_cold_limiter() {
        let (server, api) = ScriptedRiotServer::start(vec![], unlimited_config()).await;

        assert_eq!(Some(200), try_send(&api).await);
        assert_eq!(Some(200), try_send(&api).await);
        assert_eq!(2, server.request_count());
    }

    #[tokio::test]
    async fn rate_limiting_off_sends_after_a_throttle_429() {
        let (server, api) =
            ScriptedRiotServer::start(vec![service_throttle(1)], unlimited_config()).await;

        assert_eq!(Some(429), try_send(&api).await);
        assert_eq!(Some(200), try_send(&api).await);

        // With rate limiting on, this blocking call would wait out the 1.5 s park.
        let started = Instant::now();
        let request = api.request(Method::GET, PLATFORM, "/");
        api.execute_raw(METHOD_ID, PLATFORM, request).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));

        assert_eq!(3, server.request_count());
    }

    #[tokio::test]
    async fn rate_limiting_off_retry_waits_out_retry_after() {
        let (server, api) =
            ScriptedRiotServer::start(vec![service_throttle(0)], unlimited_config().set_retries(1))
                .await;

        let started = Instant::now();
        assert_eq!(Some(200), try_send(&api).await);
        // `Retry-After: 0` plus the 0.5 s rounding allowance.
        assert!(started.elapsed() >= Duration::from_millis(500));
        assert_eq!(2, server.request_count());
    }
}
