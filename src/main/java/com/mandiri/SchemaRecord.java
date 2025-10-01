package com.mandiri;

import java.io.Serializable;

public class SchemaRecord implements Serializable {
	
    private static final long serialVersionUID = 1L;
    
	private int id;
    private String description;
    
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	public SchemaRecord(int id, String description) {
		super();
		this.id = id;
		this.description = description;
	}

	@Override
	public String toString() {
		return "SchemaRecord [id=" + id + ", description=" + description + "]";
	}
	
    
    

   
}
