package com.mandiri;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

public class Schema1MapStore implements MapStore<Integer, SchemaRecord>, MapLoaderLifecycleSupport {

    private Connection connection;
    
    public Connection getConnection() {
		return connection;
	}



	public void setConnection(Connection connection) {
		this.connection = connection;
	}



	public Schema1MapStore() {
    	super();
    }
    

   
    
   

    
	@Override
    public void store(Integer key, SchemaRecord value) {
        try (Connection conn = getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO schema1_table (id, description) VALUES (?, ?) " +
                "ON CONFLICT (id) DO UPDATE SET description = EXCLUDED.description");
            stmt.setInt(1, key);
            stmt.setString(2, value.getDescription());
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void storeAll(Map<Integer, SchemaRecord> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(Integer key) {
        try (Connection conn = getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(
                "DELETE FROM schema1_table WHERE id=?");
            stmt.setInt(1, key);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
        keys.forEach(this::delete);
    }

    @Override
    public SchemaRecord load(Integer key) {
        try (Connection conn = getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, description FROM schema1_table WHERE id=?");
            stmt.setInt(1, key);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return new SchemaRecord(rs.getInt("id"), rs.getString("description"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Map<Integer, SchemaRecord> loadAll(Collection<Integer> keys) {
        Map<Integer, SchemaRecord> result = new HashMap<>();
        for (Integer key : keys) {
            SchemaRecord rec = load(key);
            if (rec != null) {
                result.put(key, rec);
            }
        }
        return result;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        List<Integer> keys = new ArrayList<>();
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id FROM schema1_table");
            while (rs.next()) {
                keys.add(rs.getInt("id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return keys;
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
