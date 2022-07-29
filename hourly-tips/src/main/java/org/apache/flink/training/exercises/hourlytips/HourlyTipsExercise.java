/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

		//throw new MissingSolutionException();

		//Note that it is possible to cascade one set of time windows after another,
		// so long as the timeframes are compatible (the second set of windows needs to have a duration that is a multiple of the first set).
		// So you can have a initial set of hour-long windows that is keyed by the driverId and use this to create a stream of
		// (endOfHourTimestamp, driverId, totalTips)
		// hourly tips per driver
		DataStream<Tuple3<Long, Long, Float>> hourlyTipsPerDriver = fares
				.keyBy(fare -> fare.driverId)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
					@Override
					public void process(Long driverId,
										ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
										Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
						float tipsPerHour = 0F;
						for (TaxiFare taxiFare : elements) {
							tipsPerHour += taxiFare.tip;
						}
						out.collect(new Tuple3(context.window().getEnd(), driverId, tipsPerHour));
					}
				});

		//and then follow this with another hour-long window (this window is not keyed) that finds the record
		// from the first window with the maximum totalTips.
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTipsPerDriver
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
				.maxBy(1);


		//(TaxiFare

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

}
