package com.mandiri;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.shaded.com.zaxxer.hikari.HikariConfig;
import com.hazelcast.shaded.com.zaxxer.hikari.HikariDataSource;

public class TxEventMapStore implements MapStore<String, TxEvent>, MapLoaderLifecycleSupport {

    private Connection connection;
    
    public Connection getConnection() {
		return connection;
	}



	public void setConnection(Connection connection) {
		this.connection = connection;
	}



	public TxEventMapStore() {
    	super();
    }
    

   
    
   

    
    @Override
    public void store(String key, TxEvent event) {
        String upsertSql = """
                INSERT INTO account_balance (
                id, cif, account_number, currency, running_balance, created_time, created_by, updated_time, updated_by
                ) VALUES (
                ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'SYSTEM', CURRENT_TIMESTAMP, 'SYSTEM'
                ) ON CONFLICT (account_number, currency)
                DO UPDATE SET running_balance = EXCLUDED.running_balance,
                updated_time = CURRENT_TIMESTAMP,
                updated_by = 'SYSTEM';
                """;
        try (PreparedStatement ps = connection.prepareStatement(upsertSql)) {
            ps.setString(1, UUID.nameUUIDFromBytes((event.getAccountNumber() + "|" + event.getCurrency()).getBytes()).toString());
            ps.setString(2, event.getCif());
            ps.setString(3, event.getAccountNumber());
            ps.setString(4, event.getCurrency());
            ps.setBigDecimal(5, event.getRunningBalance());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error storing TxEvent: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeAll(Map<String, TxEvent> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(String key) {
        // implement if needed
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        // implement if needed
    }

    @Override
    public TxEvent load(String key) {
        return null; // Not required if you only care about writes
    }

    @Override
    public Map<String, TxEvent> loadAll(Collection<String> keys) {
        return Collections.emptyMap();
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return Collections.emptyList();
    }



	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		 String DBUrl = (String) properties.get("jdbcUrl");
	        String uname = (String) properties.get("username");
	        String pwd = (String) properties.get("password");
	        try {
				this.setConnection (DriverManager.getConnection(DBUrl, uname, pwd));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	
}
