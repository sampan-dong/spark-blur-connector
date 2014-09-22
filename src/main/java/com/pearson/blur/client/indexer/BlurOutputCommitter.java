package com.pearson.blur.client.indexer;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class BlurOutputCommitter extends OutputCommitter {

	private static final Log LOG = LogFactory.getLog(BlurOutputCommitter.class);

	private Path _newIndex;
	private Configuration _configuration;
	//private TaskAttemptID _taskAttemptID;
	private Path _indexPath;
	private TableDescriptor _tableDescriptor;

	@Override
	public void commitJob(JobContext jobContext) throws IOException {
		// look through all the shards for attempts that need to be cleaned up.
		// also find all the attempts that are finished
		// then rename all the attempts jobs to commits
		LOG.info("Commiting Job [{0}]", jobContext.getJobID());
		Configuration configuration = jobContext.getConfiguration();
		Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
		LOG.info("TableOutput path [{0}]", tableOutput);
		makeSureNoEmptyShards(configuration, tableOutput);
		FileSystem fileSystem = tableOutput.getFileSystem(configuration);
		for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
			LOG.info("Checking file status [{0}] with path [{1}]", fileStatus,
					fileStatus.getPath());
			if (isShard(fileStatus)) {
				commitOrAbortJob(jobContext, fileStatus.getPath(), true);
			}
		}
		LOG.info("Commiting Complete [{0}]", jobContext.getJobID());
		super.commitJob(jobContext);
	}

	@Override
	public void abortJob(JobContext jobContext, State state) throws IOException {
		LOG.info("Abort Job [{0}]", jobContext.getJobID());
		Configuration configuration = jobContext.getConfiguration();
		Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
		makeSureNoEmptyShards(configuration, tableOutput);
		FileSystem fileSystem = tableOutput.getFileSystem(configuration);
		for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
			if (isShard(fileStatus)) {
				commitOrAbortJob(jobContext, fileStatus.getPath(), false);
			}
		}
	}

	private void commitOrAbortJob(JobContext jobContext, Path shardPath,
			boolean commit) throws IOException {
		LOG.info("CommitOrAbort [{0}] path [{1}]", commit, shardPath);
		FileSystem fileSystem = shardPath.getFileSystem(jobContext
				.getConfiguration());
		FileStatus[] listStatus = fileSystem.listStatus(shardPath,
				new PathFilter() {
					@Override
					public boolean accept(Path path) {
						LOG.info("Checking path [{0}]", path);
						if (path.getName().endsWith(".task_complete")) {
							return true;
						}
						return false;
					}
				});
		for (FileStatus fileStatus : listStatus) {
			Path path = fileStatus.getPath();
			LOG.info("Trying to commitOrAbort [{0}]", path);
			String name = path.getName();
			boolean taskComplete = name.endsWith(".task_complete");
			if (fileStatus.isDir()) {
				String taskAttemptName = getTaskAttemptName(name);
				if (taskAttemptName == null) {
					LOG.info("Dir name [{0}] not task attempt", name);
					continue;
				}
				TaskAttemptID taskAttemptID = TaskAttemptID
						.forName(taskAttemptName);
				if (taskAttemptID.getJobID().toString().equals(jobContext.getJobID().toString())) {
					if (commit) {
						if (taskComplete) {
							fileSystem.rename(path, new Path(shardPath,taskAttemptName + ".commit"));
							LOG.info("Committing [{0}] in path [{1}]",
									taskAttemptID, path);
						} else {
							fileSystem.delete(path, true);
							LOG.info("Deleteing tmp dir [{0}] in path [{1}]",
									taskAttemptID, path);
						}
					} else {
						fileSystem.delete(path, true);
						LOG.info(
								"Deleteing aborted job dir [{0}] in path [{1}]",
								taskAttemptID, path);
					}
				} else {
					LOG.warn(
							"TaskAttempt JobID [{0}] does not match JobContext JobId [{1}]",
							taskAttemptID.getJobID(), jobContext.getJobID());
				}
			}
		}
	}

	private String getTaskAttemptName(String name) {
		int lastIndexOf = name.lastIndexOf('.');
		if (lastIndexOf < 0) {
			return null;
		}
		return name.substring(0, lastIndexOf);
	}

	private void makeSureNoEmptyShards(Configuration configuration,
			Path tableOutput) throws IOException {
		FileSystem fileSystem = tableOutput.getFileSystem(configuration);
		TableDescriptor tableDescriptor = BlurOutputFormat
				.getTableDescriptor(configuration);
		int shardCount = tableDescriptor.getShardCount();
		for (int i = 0; i < shardCount; i++) {
			String shardName = BlurUtil.getShardName(i);
			fileSystem.mkdirs(new Path(tableOutput, shardName));
		}
	}

	private boolean isShard(FileStatus fileStatus) {
		return isShard(fileStatus.getPath());
	}

	private boolean isShard(Path path) {
		return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
	}

	@Override
	public void abortTask(TaskAttemptContext context)
			throws IOException {
		setup(context);
		FileSystem fileSystem = _newIndex.getFileSystem(_configuration);
		LOG.info("abortTask - Deleting [{0}]", _newIndex);
		fileSystem.delete(_newIndex, true);
	}

	private void setup(TaskAttemptContext context) throws IOException {
		LOG.info("Running task setup.");
		_configuration = context.getConfiguration();
		_tableDescriptor = BlurCustomOutputFormat
				.getTableDescriptor(_configuration);
		int shardCount = _tableDescriptor.getShardCount();
		int attemptId = context.getTaskAttemptID().getTaskID().getId();
		int shardId = attemptId % shardCount;
		//_taskAttemptID = context.getTaskAttemptID();
		Path tableOutput = BlurCustomOutputFormat.getOutputPath(_configuration);
		String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX,
				shardId);
		_indexPath = new Path(tableOutput, shardName);
		_newIndex = new Path(_indexPath, context.getTaskAttemptID().toString() + ".tmp");
	}

	@Override
	public void commitTask(
			TaskAttemptContext context)
			throws IOException {
		setup(context);
		FileSystem fileSystem = _newIndex.getFileSystem(_configuration);
		if (fileSystem.exists(_newIndex) && !fileSystem.isFile(_newIndex)) {
			Path dst = new Path(_indexPath, context.getTaskAttemptID().toString().toString()
					+ ".task_complete");
			LOG.info("Committing [{0}] to [{1}]", _newIndex, dst);
			fileSystem.rename(_newIndex, dst);
		} else {
			throw new IOException("Path [" + _newIndex
					+ "] does not exist, can not commit.");
		}
	}

	@Override
	public boolean needsTaskCommit(
			TaskAttemptContext context)
			throws IOException {
		int numReduceTasks = context.getNumReduceTasks();
		TaskAttemptID taskAttemptID = context.getTaskAttemptID();
		return taskAttemptID.isMap() && numReduceTasks != 0 ? false : true;
	}

	@Override
	public void setupJob(JobContext context) throws IOException {
		LOG.info("Running Job setup.");
	}

	@Override
	public void setupTask(TaskAttemptContext context)
			throws IOException {
		LOG.info("Running Task setup.");
	}

}
