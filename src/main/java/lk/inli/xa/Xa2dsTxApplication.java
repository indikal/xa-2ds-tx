package lk.inli.xa;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.XADataSourceWrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class Xa2dsTxApplication {

	public static void main(String[] args) {
		SpringApplication.run(Xa2dsTxApplication.class, args);
	}

	private final XADataSourceWrapper wrapper;
	
	public Xa2dsTxApplication(XADataSourceWrapper wrapper) {
		this.wrapper = wrapper;
	}

	@Bean
	@ConfigurationProperties(prefix = "a")
	DataSource a() throws Exception {
		return this.wrapper.wrapDataSource(dataSource("a"));
	}

	@Bean
	@ConfigurationProperties(prefix = "b")
	DataSource b() throws Exception {
		return this.wrapper.wrapDataSource(dataSource("b"));
	}

	@Bean
	DataSourceInitializer aInit(DataSource a) {
		return init(a, "a");
	}

	@Bean
	DataSourceInitializer bInit(DataSource b) {
		return init(b, "b");
	}

	private DataSourceInitializer init(DataSource a, String name) {
		DataSourceInitializer dsi = new DataSourceInitializer();
		dsi.setDataSource(a);
		dsi.setDatabasePopulator(new ResourceDatabasePopulator(new ClassPathResource(name + ".sql")));
		return dsi;
	}

	private JdbcDataSource dataSource(String schema) {
		JdbcDataSource jdbcDataSource = new JdbcDataSource();
		jdbcDataSource.setUrl("jdbc:h2:./" + schema);
		jdbcDataSource.setUser("sa");
		jdbcDataSource.setPassword("");

		return jdbcDataSource;
	}

	@RestController
	public static class XaApiRestController {
		private JdbcTemplate a, b;

		public XaApiRestController(DataSource a, DataSource b) {
			this.a = new JdbcTemplate(a);
			this.b = new JdbcTemplate(b);
		}

		@GetMapping(path = "pets")
		public Collection<String> pets() {
			return this.a.query("select * from PET", (ResultSet resultSet, int i) -> {
				return resultSet.getString("nickname");
			});
		}

		@GetMapping(path = "messages")
		public Collection<String> messages() {
			return this.b.query("select * from MESSAGE", (ResultSet resultSet, int i) -> {
				return resultSet.getString("MESSAGE");
			});
		}

		@PostMapping
		@Transactional
		public void write(@RequestBody Map<String, String> payload, @RequestParam Optional<Boolean> rollback) {
			String name = payload.get("name");
			String msg = "Hello, " + name + "!";

			this.a.update("insert into PET (ID, NICKNAME) values(?, ?)", UUID.randomUUID().toString(), name);

			this.b.update("insert into MESSAGE(ID, MESSAGE) values(?, ?)", UUID.randomUUID().toString(), msg);

			if (rollback.orElse(false)) {
				throw new RuntimeException("Couldn't write the data to the databases!");
			}
		}
	}
}
