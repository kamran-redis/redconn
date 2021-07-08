package com.redislabs.redconn;

import io.lettuce.core.*;
import io.lettuce.core.ClientOptions.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;
import java.time.Duration;

@SpringBootApplication
@Slf4j
public class RedconnApplication implements CommandLineRunner {

	private static final String DNS_CACHE_TTL = "networkaddress.cache.ttl";
	private static final String DNS_CACHE_NEGATIVE_TTL = "networkaddress.cache.negative.ttl";
	@Autowired
	private RedconnConfiguration config;

	public static void main(String[] args) {
		SpringApplication.run(RedconnApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("Setting {}={}", DNS_CACHE_TTL, config.getDnsTtl());
		Security.setProperty(DNS_CACHE_TTL, config.getDnsTtl());
		log.info("Setting {}={}", DNS_CACHE_NEGATIVE_TTL, config.getDnsNegativeTtl());
		Security.setProperty(DNS_CACHE_NEGATIVE_TTL, config.getDnsNegativeTtl());
		switch (config.getDriver()) {
			case Lettuce:
				runLettuce();
				break;

			case DNS:
				runDNS();
				break;

			default:
				runJedis();
				break;
		}
	}

	private void runDNS() throws InterruptedException {
		String previous = null;
		while(true) {
			try {
				String current = getHostAddress(config.getHost());
				log.info("IP address is {}", current);
				if (previous == null) {
					previous = current;
				}
				if (!previous.equals(current)) {
					log.info("IP address changed from {}  to {}", previous, current);
				}
				previous = current;
				Thread.sleep(config.getDnsSleep());
			} catch (Exception e) {
				//ignore
			}
		}
	}

	private void runJedis() throws InterruptedException {
		SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
		SSLParameters sslParameters = new SSLParameters();
		sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
		HostnameVerifier hostnameVerifier = null;
		GenericObjectPoolConfig<redis.clients.jedis.Jedis> poolConfig = new GenericObjectPoolConfig<>();
		log.info("Connecting using Jedis with {}", config);
		JedisPool jedisPool = new JedisPool(poolConfig, config.getHost(), config.getPort(),
				config.getConnectionTimeout(), config.getSocketTimeout(), config.getPassword(), config.getDatabase(),
				config.getClientName(), config.isSsl(), sslSocketFactory, sslParameters, hostnameVerifier);
		try {
			Jedis jedis = jedisPool.getResource();
			log.info("Connected to {}", jedis.getClient().getHost());
			int numKeys = config.getNumKeys();
			for (int index = 0; index < numKeys; index++) {
				jedis.set("key:" + index, "value" + index);
			}
			while (true) {
				try {
					for (int index = 0; index < numKeys; index++) {
						String value = jedis.get("key:" + index);
						if (value == null || !value.equals("value" + index)) {
							log.error("Incorrect value returned: " + value);
						}
					}
					if (log.isDebugEnabled()) {
						log.debug("Successfully performed GET on all {} keys", numKeys);
					}
					Thread.sleep(config.getSleep().getGet());
				} catch (Exception e) {
					jedis.close();
					jedis = null;
					log.info("Disconnected");
					long startTime = System.nanoTime();
					while (jedis == null) {
						try {
							jedis = jedisPool.getResource();
						} catch (Exception e2) {
							Thread.sleep(config.getSleep().getReconnect());
						}
					}
					long durationInNanos = System.nanoTime() - startTime;
					double durationInSec = (double) Duration.ofNanos(durationInNanos).toMillis() / 1000;
					log.info("Reconnected after {} seconds", String.format("%.3f", durationInSec));
				}
			}
		} finally {
			jedisPool.close();
			log.info("Closed");
		}
	}

	private void runLettuce() throws InterruptedException {
		/*ClientResources clientResources = ClientResources.builder()
				.nettyCustomizer(new NettyCustomizer() {
					@Override
					public void afterBootstrapInitialized(Bootstrap bootstrap) {
						bootstrap.option(EpollChannelOption.TCP_KEEPIDLE, 10);
						bootstrap.option(EpollChannelOption.TCP_KEEPINTVL, 2);
						bootstrap.option(EpollChannelOption.TCP_KEEPCNT, 2);
						bootstrap.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPIDLE), 10);
						bootstrap.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPINTERVAL), 2);
						bootstrap.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPCOUNT), 2);
					}
				})
				.build();
		RedisClient client = RedisClient.create(clientResources, RedisURI.create(config.getHost(), config.getPort()));*/

		RedisClient client = RedisClient.create(RedisURI.create(config.getHost(), config.getPort()));
		log.info("IP address for host {} is {}", config.getHost(), getHostAddress(config.getHost()));

		//Defualt is 60 seconds in lettuce
		log.info("Setting Lettuce Default  Timeout={}",  config.getSocketTimeout());
		client.setDefaultTimeout(Duration.ofSeconds(config.getSocketTimeout()));

		client.setOptions(getLettuceClientOptions());
		StatefulRedisConnection<String, String> connection = client.connect();
		log.info("Connected to {}", config.getHost());

		int numKeys = config.getNumKeys();
		try {
		for (int index = 0; index < numKeys; index++) {
			connection.sync().set("key:" + index, "value" + index);
		}

		while (true) {
			try {
				for (int index = 0; index < numKeys; index++) {
					String value = connection.sync().get("key:" + index);
					if (value == null || !value.equals("value" + index)) {
						log.error("Incorrect value returned: " + value);
					}
					log.info("Got key {} ", "key:" + index);
					Thread.sleep(config.getSleep().getGet());
				}
				log.info("Successfully performed GET on all {} keys", numKeys);
				Thread.sleep(config.getSleep().getGet());
			} catch (Exception e) {
				log.error("Disconnected");
				long startTime = System.nanoTime();
				while (true)  {
					try {
						log.error("Trying to reconnect....");
						String  resp  = connection.sync().ping();
						log.info("Response {}", resp);
						break;
					} catch (Exception e2) {
						Thread.sleep(config.getSleep().getReconnect());
					}

				}
				long durationInNanos = System.nanoTime() - startTime;
				double durationInSec = (double) Duration.ofNanos(durationInNanos).toMillis() / 1000;
				log.info("Reconnected after {} seconds", String.format("%.3f", durationInSec));
			}
		}
	} finally {
		connection.close();
		client.shutdown();
		log.info("Closed");
	}
	}

	private ClientOptions getLettuceClientOptions() {
		Builder builder = ClientOptions.builder();
		if (config.isSsl()) {
			builder.sslOptions(getSslOptions());
		}
		//default is true in lettuce
		//builder.autoReconnect(true);
		SocketOptions.Builder  socketOptionsBuilder = SocketOptions.builder();
		//default is 10 seconds in lettue
		socketOptionsBuilder.connectTimeout(Duration.ofSeconds(config.getConnectionTimeout()));

		socketOptionsBuilder.keepAlive(true);
		/**
		 * SocketOptions.KeepAliveOptions is only available from lettuce 6.1 use Linux OS settting
		 * echo 15 > /proc/sys/net/ipv4/tcp_keepalive_time
		 * echo 2 > /proc/sys/net/ipv4/tcp_keepalive_intvl
		 * echo 2 > /proc/sys/net/ipv4/tcp_keepalive_probes
		 */
		//SocketOptions.KeepAliveOptions.Builder keepAliveBuilder = SocketOptions.KeepAliveOptions.builder();
		//keepAliveBuilder.count(2).enable(true).idle(Duration.ofSeconds(15)).interval(Duration.ofSeconds(2));
		//socketOptionsBuilder.keepAlive(keepAliveBuilder.build());

		//default is true
		//socketOptionsBuilder.tcpNoDelay(true);
		builder.socketOptions(socketOptionsBuilder.build());
		return builder.build();
	}

	private SslOptions getSslOptions() {
		SslOptions.Builder builder = SslOptions.builder();
		switch (config.getSslProvider()) {
		case OpenSsl:
			builder.openSslProvider();
			break;
		default:
			builder.jdkSslProvider();
			break;
		}
		if (config.getKeystore() != null) {
			KeystoreConfiguration keystoreConfig = config.getKeystore();
			File file = new File(keystoreConfig.getFile());
			if (keystoreConfig.getPassword() == null) {
				builder.keystore(file);
			} else {
				builder.keystore(file, keystoreConfig.getPassword().toCharArray());
			}
		}
		if (config.getTruststore() != null) {
			TruststoreConfiguration truststoreConfig = config.getTruststore();
			File file = new File(truststoreConfig.getFile());
			if (truststoreConfig.getPassword() == null) {
				builder.truststore(file);
			} else {
				builder.truststore(file, truststoreConfig.getPassword());
			}
		}
		return builder.build();
	}

	private String getHostAddress(String host) {
		try {

			InetAddress inetHost = InetAddress.getByName(host);
			return inetHost.getHostAddress();
		} catch(UnknownHostException ex) {
			return "Unknown Host";
		}
	}
}
