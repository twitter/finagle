
##### Sample Protobuf based RPC (see RpcIntegrationTest for more samples)


	    RpcFactory factory = new RpcFactoryImpl();

	    ServerBuilder serverBuilder = ServerBuilder.get()
			.maxConcurrentRequests(10);

	    ClientBuilder clientBuilder = ClientBuilder
			.get()
			.hosts(String.format("localhost:%s", port))
			.hostConnectionLimit(1)
			.retries(2)
			.requestTimeout(
					Duration.apply(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS));


        SampleWeatherServiceImpl service = new SampleWeatherServiceImpl(80,
				null);
		RpcServer server = factory.createServer(serverBuilder, port, service,
				executorService);
		WeatherService stub = factory.<WeatherService> createStub(
				clientBuilder, WeatherService.newStub(null), executorService);

		RpcControllerWithOnFailureCallback controller = (RpcControllerWithOnFailureCallback) factory
				.createController();

		final CountDownLatch l = new CountDownLatch(1);
		final AtomicInteger result = new AtomicInteger();
		GetWeatherForecastRequest request = GetWeatherForecastRequest
				.newBuilder().setZip("80301").build();
		stub.getWeatherForecast(
				controller.onFailure(new RpcCallback<Throwable>() {

					@Override
					public void run(Throwable e) {
					}
				}), request, new RpcCallback<GetWeatherForecastResponse>() {

					@Override
					public void run(GetWeatherForecastResponse resp) {
						result.set(resp.getTemp());
						l.countDown();
					}
				});

		l.await(CLIENT_TIMEOUT_SECONDS + 2, TimeUnit.SECONDS);
		server.close(new Duration(TimeUnit.SECONDS.toNanos(1)));

		assertEquals(service.getTemperature(), result.get());
		
		


