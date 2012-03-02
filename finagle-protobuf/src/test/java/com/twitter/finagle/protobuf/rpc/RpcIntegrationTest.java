package com.twitter.finagle.protobuf.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.twitter.finagle.protobuf.rpc.RpcControllerWithOnFailureCallback;
import com.twitter.finagle.protobuf.rpc.RpcFactory;
import com.twitter.finagle.protobuf.rpc.RpcServer;
import com.twitter.finagle.protobuf.rpc.impl.RpcFactoryImpl;
import com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherRequest;
import com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetHistoricWeatherResponse;
import com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastRequest;
import com.twitter.finagle.protobuf.rpc.SampleServiceProtos.GetWeatherForecastResponse;
import com.twitter.finagle.protobuf.rpc.SampleServiceProtos.WeatherService;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.util.Duration;

public class RpcIntegrationTest {

	private static final int MILLIS_PER_SEC = 1000;

	private static final int CLIENT_TIMEOUT_SECONDS = 1;

	private static final int RPC_OVERHEAD_MILLIS = 300;

	private int port = 8080;

	private ExecutorService executorService = Executors.newFixedThreadPool(2);

	private RpcFactory factory = new RpcFactoryImpl();

	private ServerBuilder serverBuilder = ServerBuilder.get()
			.maxConcurrentRequests(10);

	private ClientBuilder clientBuilder = ClientBuilder
			.get()
			.hosts(String.format("localhost:%s", port))
			.hostConnectionLimit(1)
			.retries(2)
			.requestTimeout(
					Duration.apply(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

	@Test
	public void testSuccessfulRequest() throws InterruptedException {

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
	}

	@Test
	public void testMethodWithResponseTimeout() throws InterruptedException {

		SampleWeatherServiceImpl service = new SampleWeatherServiceImpl(80,
				new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						// force timeout
						Thread.sleep((CLIENT_TIMEOUT_SECONDS * 10) * 1000);
						return null;
					}
				});
		RpcServer server = factory.createServer(serverBuilder, port, service,
				executorService);
		WeatherService stub = factory.<WeatherService> createStub(
				clientBuilder, WeatherService.newStub(null), executorService);
		RpcControllerWithOnFailureCallback controller = (RpcControllerWithOnFailureCallback) factory
				.createController();

		final AtomicBoolean processed = new AtomicBoolean(false);

		final CountDownLatch l = new CountDownLatch(1);
		GetHistoricWeatherRequest request = GetHistoricWeatherRequest
				.newBuilder().setZip("80301").build();
		long callStartTime = System.currentTimeMillis();
		stub.getHistoricWeather(
				controller.onFailure(new RpcCallback<Throwable>() {

					@Override
					public void run(Throwable e) {
						if (e instanceof TimeoutException) {
							processed.set(true);
							l.countDown();
						}
					}
				}), request, new RpcCallback<GetHistoricWeatherResponse>() {

					@Override
					public void run(GetHistoricWeatherResponse resp) {
					}
				});

		l.await(CLIENT_TIMEOUT_SECONDS + 2, TimeUnit.SECONDS);
		long methodCallDuration = (System.currentTimeMillis() - callStartTime);
		assertTrue(
				"Method call duration ~= client timeout " + methodCallDuration,
				Math.abs(methodCallDuration - CLIENT_TIMEOUT_SECONDS
						* MILLIS_PER_SEC) < RPC_OVERHEAD_MILLIS);

		try {
			assertTrue("Expected timeout.", processed.get());
		} finally {
			server.close(new Duration(TimeUnit.SECONDS.toNanos(1)));
		}
	}

	@Test
	public void testMethodWithException() throws InterruptedException {

		SampleWeatherServiceImpl service = new SampleWeatherServiceImpl(80,
				new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						throw new RuntimeException(
								"Cannot connect to the database...");
					}
				});
		RpcServer server = factory.createServer(serverBuilder, port, service,
				executorService);
		WeatherService stub = factory.<WeatherService> createStub(
				clientBuilder, WeatherService.newStub(null), executorService);
		RpcControllerWithOnFailureCallback controller = (RpcControllerWithOnFailureCallback) factory
				.createController();

		final AtomicBoolean processed = new AtomicBoolean(false);

		final CountDownLatch l = new CountDownLatch(1);

		GetHistoricWeatherRequest request = GetHistoricWeatherRequest
				.newBuilder().setZip("80301").build();
		long callStartTime = System.currentTimeMillis();
		stub.getHistoricWeather(
				controller.onFailure(new RpcCallback<Throwable>() {

					@Override
					public void run(Throwable e) {
						if (e instanceof RuntimeException) {
							processed.set(true);
							l.countDown();
						}
					}
				}), request, new RpcCallback<GetHistoricWeatherResponse>() {

					@Override
					public void run(GetHistoricWeatherResponse resp) {
					}
				});

		l.await(CLIENT_TIMEOUT_SECONDS + 2, TimeUnit.SECONDS);
		long methodCallDuration = (System.currentTimeMillis() - callStartTime);
		assertTrue("The exception occured BEFORE the client timeout "
				+ methodCallDuration,
				methodCallDuration < CLIENT_TIMEOUT_SECONDS * MILLIS_PER_SEC);

		try {
			assertTrue("Expected failure.", processed.get());
		} finally {
			server.close(new Duration(TimeUnit.SECONDS.toNanos(1)));
		}
	}

	public class SampleWeatherServiceImpl extends WeatherService {

		private final int temperature;
		private final Callable<?> getHistoricWeather;

		public SampleWeatherServiceImpl(int temperature,
				Callable<?> getHistoricWeather) {
			super();
			this.temperature = temperature;
			this.getHistoricWeather = getHistoricWeather;
		}

		public int getTemperature() {
			return temperature;
		}

		@Override
		public void getWeatherForecast(RpcController controller,
				GetWeatherForecastRequest request,
				RpcCallback<GetWeatherForecastResponse> done) {
			done.run(GetWeatherForecastResponse.newBuilder()
					.setTemp(temperature).build());
		}

		@Override
		public void getHistoricWeather(RpcController controller,
				GetHistoricWeatherRequest request,
				RpcCallback<GetHistoricWeatherResponse> done) {
			if (this.getHistoricWeather != null) {
				try {
					this.getHistoricWeather.call();
				} catch (Exception e) {
					throw Throwables.propagate(e);
				}
			}
		}

	}
}
