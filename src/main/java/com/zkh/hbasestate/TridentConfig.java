package com.zkh.hbasestate;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateType;

public class TridentConfig<T> extends TupleTableConfig{
	private static final long serialVersionUID = 1L;
	private int stateCacheSize = 1000;
	private Serializer stateSerializer;


	public Serializer getStateSerializer() {
		return stateSerializer;
	}

	public void setStateSerializer(Serializer stateSerializer) {
		this.stateSerializer = stateSerializer;
	}

	public static final Map<StateType,Serializer> DEFAULT_STATE = 
    		new HashMap<StateType,Serializer>(){
    	{
    		put(StateType.NON_TRANSACTIONAL,new JSONNonTransactionalSerializer());
    		put(StateType.TRANSACTIONAL,new JSONTransactionalSerializer());
    		put(StateType.OPAQUE,new JSONOpaqueSerializer());
    	}
    };
    
    public TridentConfig(String tableName){
    	super(tableName);
    }

    public TridentConfig(String tableName,String rowKey,String timestamp){
    	super(tableName, rowKey,timestamp);
    }

	public int getStateCacheSize() {
		return stateCacheSize;
	}

	public void setStateCacheSize(int stateCacheSize) {
		this.stateCacheSize = stateCacheSize;
	}
    
}
