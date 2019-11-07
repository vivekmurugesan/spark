package com.test.graph.operations;

import java.io.Serializable;
import java.util.Set;

/**
 * 
 * @author vivek
 *
 */
public class ConnectedComponent implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private int componentId;
	private Set<Integer> members;
	private int size;
	
	public ConnectedComponent(int componentId, Set<Integer> members, int size) {
		super();
		this.componentId = componentId;
		this.members = members;
		this.size = size;
	}
	
	public int getComponentId() {
		return componentId;
	}
	public void setComponentId(int componentId) {
		this.componentId = componentId;
	}
	public Set<Integer> getMembers() {
		return members;
	}
	public void setMembers(Set<Integer> members) {
		this.members = members;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return "ConnectedComponent [componentId=" + componentId + ", size=" + size + ", members=" + members + "]";
	}
	
	
	
}
