package com.pearson.blur.client.indexer;

import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;

public class BlurSparkPartitioner extends HashPartitioner {


	private static final long serialVersionUID = 9853263327838L;
	
	int totalShard;
	
	public BlurSparkPartitioner(int partitions) {
		
		super(partitions);
		totalShard = partitions;
	}

	@Override
	public int getPartition(Object key){
		
		if(key instanceof Text){
			
			 return (key.hashCode() & Integer.MAX_VALUE) % totalShard;
		}else{
			return super.getPartition(key);
		}
	}
}
