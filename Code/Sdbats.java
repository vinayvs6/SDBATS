//package spdsProject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Sdbats {

	public static void main(String... s) throws Exception {
		Scanner input = new Scanner(System.in);
		System.out.println("Enter the number of available Processors");
		int processors = input.nextInt();
		System.out.println("Enter the number of Tasks");
		int tasks = input.nextInt();
		int exitNode = 0;
		int entryNode = 0;
		int[][] computationMatrix = new int[tasks][processors];
		int[][] communicationMatrix = new int[tasks][tasks];
		Map<Integer, Integer> taskProcessorMap = new HashMap<Integer, Integer>();
		Map<Integer, Integer> taskEndTime = new HashMap<Integer, Integer>();
		Map<Integer, List<Integer>> taskAllocationMap = new HashMap<Integer, List<Integer>>();
		for (int i = 0; i < processors; i++) {
			List<Integer> temp = new ArrayList<Integer>();
			taskAllocationMap.put(i, temp);
		}
		int[] latestExecutionTime = new int[processors];
		// Index for Final Answer i.e List of List of Integers
		// 0 = task_name
		// 1= processor_allocated
		// 2 = earliest_start_time
		// 3 = earliest_end_Time
		List<List<Integer>> finalAnswer = new ArrayList<List<Integer>>();
		input.nextLine();
		input.close();

		File file = new File("dataW.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		int counter = 0;
		while ((line = br.readLine()) != null) {
			String[] computationOverhead = line.split(",");
			for (int i = 0; i < processors; i++) {
				computationMatrix[counter][i] = Integer.parseInt(computationOverhead[i].trim());
			}
			counter++;
		}

		File fileD = new File("data.txt");
		BufferedReader brD = new BufferedReader(new FileReader(fileD));
		String lineD;
		int counterD = 0;
		while ((lineD = brD.readLine()) != null) {
			String[] communicationOverhead = lineD.split(",");
			for (int i = 0; i < tasks; i++) {
				communicationMatrix[counterD][i] = Integer.parseInt(communicationOverhead[i].trim());
			}
			counterD++;
		}

		System.out.println("\n\n\nFinding Exit node...........");
		exitNode = getExitTask(communicationMatrix, tasks);
		System.out.println("Exit node is  :  " + (exitNode + 1));
		System.out.println("Finding Entry node...........");
		entryNode = getEntryTask(communicationMatrix, tasks);
		System.out.println("Entry node is  :  " + (entryNode + 1));

		Map<Integer, Double> upwardRank = new HashMap<Integer, Double>();
		List<Integer> taskList = new ArrayList<Integer>();
		List<Integer> parentTaskList = new ArrayList<Integer>();
		List<Integer> childTaskList = new ArrayList<Integer>();
		List<Integer> unTraversedNode = new ArrayList<Integer>();
		List<Integer> priorityList = new ArrayList<Integer>();

		for (int i = 0; i < tasks; i++)
			taskList.add(i);
		int node = exitNode;

		for (int i = 0; i < tasks; i++) {
			for (int j = 0; j < tasks; j++) {
				if (communicationMatrix[node][j] != 0) {
					childTaskList.add(j);
				}
			}

			for (int j = 0; j < tasks; j++) {
				if (communicationMatrix[j][node] != 0) {
					if (!parentTaskList.contains(j))
						parentTaskList.add(j);
				}
			}

			for (Integer parent : parentTaskList) {
				if (!unTraversedNode.contains(parent))
					unTraversedNode.add(parent);
			}

			List<Double> costRankOfAllSuccessor = new ArrayList<Double>();
			for (Integer successor : childTaskList) {
				double rank = upwardRank.get(successor);
				double communicationCost = communicationMatrix[node][successor];
				costRankOfAllSuccessor.add(rank + communicationCost);
			}

			double maxCostRankOfAllSuccessor = 0;

			if (costRankOfAllSuccessor.size() != 0) {
				Collections.sort(costRankOfAllSuccessor);
				maxCostRankOfAllSuccessor = costRankOfAllSuccessor.get(costRankOfAllSuccessor.size() - 1);
			}

			double standardDeviationExecutionTime = getStandardDeviation(computationMatrix[node]);
			upwardRank.put(node, maxCostRankOfAllSuccessor + standardDeviationExecutionTime);

			if (unTraversedNode.size() != 0)
				node = unTraversedNode.remove(0);
			childTaskList.clear();
			parentTaskList.clear();

		}

		List<Double> rankValues = new ArrayList<>(upwardRank.values());
		Collections.sort(rankValues, new Comparator<Double>() {
			@Override
			public int compare(Double first, Double second) {
				return (int) (second - first);
			}
		});

		for (Double val : rankValues) {
			for (Map.Entry<Integer, Double> entry : upwardRank.entrySet()) {
				double mapVal = entry.getValue();
				if (mapVal == val) {
					priorityList.add(entry.getKey() + 1);
					break;
				}
			}
		}

		System.out.println("\n\n\nThe order of priority for the task is as follows :");
		for (Integer val : priorityList) {
			System.out.println(val);
		}
		
		System.out.println("\n\nThe Table Calculating Upward rank is as Follows");
		System.out.println("Task            UpwardRank");
		System.out.println("---------------------------");
		for(Map.Entry<Integer, Double> ent : upwardRank.entrySet()) {
			int task = ent.getKey();
			double value = ent.getValue();
			System.out.println(task+"               "+value);
		}
		

		for (int i = 0; i < priorityList.size(); i++) {
			priorityList.set(i, priorityList.get(i) - 1);
		}

		// Scheduling

		int entryNodeMinValue = getMin(computationMatrix[0]);
		int[] deltaMatrix = new int[processors];
		for (int i = 0; i < processors; i++) {
			deltaMatrix[i] = computationMatrix[0][i] - entryNodeMinValue;
		}

		while (priorityList.size() != 0) {
			int taskSelected = priorityList.remove(0);
			List<List<Integer>> allPossibleProcessorCombinations = new ArrayList<List<Integer>>();
			for (int i = 0; i < processors; i++) {
				int processorRunningTime = computationMatrix[taskSelected][i];
				int earliestStartingTime = getEarliestStartingTime(taskSelected, communicationMatrix, tasks, entryNode,
						computationMatrix, taskProcessorMap, i, taskAllocationMap, latestExecutionTime, taskEndTime);
				List<Integer> individualList = new ArrayList<Integer>();
				individualList.add(taskSelected);
				individualList.add(earliestStartingTime);
				individualList.add(processorRunningTime + earliestStartingTime);
				individualList.add(i);
				allPossibleProcessorCombinations.add(individualList);
			}

			Collections.sort(allPossibleProcessorCombinations, new Comparator<List<Integer>>() {
				@Override
				public int compare(List<Integer> firstList, List<Integer> secondList) {
					return firstList.get(2) - secondList.get(2);
				}
			});

			int minimumEarliestFinishTimeProcessor = allPossibleProcessorCombinations.get(0).get(3);

			if (taskSelected == entryNode) {
				for (int m = 0; m < processors; m++) {
					List<Integer> answerList = new ArrayList<Integer>();
					answerList.add(taskSelected);
					answerList.add(m);
					answerList.add(0);
					answerList.add(computationMatrix[0][m]);
					finalAnswer.add(answerList);
				}
			} else {
				List<Integer> answerList = new ArrayList<Integer>();
				answerList.add(taskSelected);
				answerList.add(minimumEarliestFinishTimeProcessor);
				answerList.add(allPossibleProcessorCombinations.get(0).get(1));
				answerList.add(allPossibleProcessorCombinations.get(0).get(2));
				taskProcessorMap.put(taskSelected, minimumEarliestFinishTimeProcessor);
				taskEndTime.put(taskSelected, allPossibleProcessorCombinations.get(0).get(2));
				List<Integer> temp = taskAllocationMap.get(minimumEarliestFinishTimeProcessor);
				latestExecutionTime[minimumEarliestFinishTimeProcessor] = allPossibleProcessorCombinations.get(0).get(2)
						- allPossibleProcessorCombinations.get(0).get(1);
				temp.add(allPossibleProcessorCombinations.get(0).get(1));
				finalAnswer.add(answerList);
			}
		}

		Collections.sort(finalAnswer, new Comparator<List<Integer>>() {
			@Override
			public int compare(List<Integer> firstList, List<Integer> secondList) {
				return firstList.get(0) - secondList.get(0);
			}
		});

		System.out.println("\n\n\nThe generated Schedule is a follows :");

		for (List<Integer> rowAns : finalAnswer) {
			if (rowAns.get(0) == 0) {
				System.out.println(
						"\n\n\nTask " + (rowAns.get(0) + 1) + " is scheduled on Processor " + (rowAns.get(1) + 1));
				System.out.println("Actual Start Time = " + rowAns.get(2));
				System.out.println("Actual Finish Time = " + rowAns.get(3));
			} else {
				System.out.println(
						"\n\n\nTask " + (rowAns.get(0) + 1) + " is scheduled on Processor " + (rowAns.get(1) + 1));
				System.out.println("Actual Start Time = " + (rowAns.get(2) + deltaMatrix[rowAns.get(1)]));
				System.out.println("Actual Finish Time = " + (rowAns.get(3) + deltaMatrix[rowAns.get(1)]));
			}

		}

	}

	private static int getEarliestStartingTime(int taskSelected, int[][] communicationMatrix, int tasks, int entryNode,
			int[][] computationMatrix, Map<Integer, Integer> taskProcessorMap, int childProcessor,
			Map<Integer, List<Integer>> taskAllocationMap, int[] latestExecutionTime,
			Map<Integer, Integer> taskEndTime) {
		if (taskSelected == entryNode)
			return 0;
		List<Integer> parentList = new ArrayList<Integer>();
		parentList = getParents(taskSelected, communicationMatrix, tasks);
		List<Integer> allPossibleParentTime = new ArrayList<Integer>();
		for (Integer parent : parentList) {
			int parentActualFinishTime = 0;
			if (parent == 0)
				parentActualFinishTime = getEarliestStartingTime(parent, communicationMatrix, tasks, entryNode,
						computationMatrix, taskProcessorMap, childProcessor, taskAllocationMap, latestExecutionTime,
						taskEndTime) + getMin(computationMatrix[parent]);
			else {
				parentActualFinishTime = taskEndTime.get(parent);
			}

			int parentCommunicationTime = 0;
			if (!(parent == 0))
				parentCommunicationTime = taskProcessorMap.get(parent) == childProcessor ? 0
						: communicationMatrix[parent][taskSelected];
			int finalCost = parentActualFinishTime + parentCommunicationTime;
			if (taskAllocationMap.get(childProcessor).contains(finalCost)) {
				finalCost = finalCost + latestExecutionTime[childProcessor];
			}
			allPossibleParentTime.add(finalCost);
		}
		Collections.sort(allPossibleParentTime);
		return allPossibleParentTime.get(allPossibleParentTime.size() - 1);
	}

	private static int getMin(int[] computationArray) {
		int min = 999999;
		for (int i = 0; i < computationArray.length; i++) {
			if (min > computationArray[i])
				min = computationArray[i];
		}
		return min;
	}

	private static List<Integer> getParents(int i, int[][] communicationMatrix, int tasks) {
		List<Integer> parentList = new ArrayList<Integer>();
		for (int j = 0; j < tasks; j++) {
			if (communicationMatrix[j][i] != 0) {
				parentList.add(j);
			}
		}
		return parentList;
	}

	private static int getEntryTask(int[][] communicationMatrix, int tasks) {
		for (int i = 0; i < tasks; i++) {
			int counter = 0;
			for (int j = 0; j < tasks; j++) {
				if (communicationMatrix[j][i] != 0)
					counter++;
			}
			if (counter == 0)
				return i;
		}
		return 0;
	}

	private static double getStandardDeviation(int[] computationTime) {
		float mean = 0;
		double standardDeviation = 0;
		for (int i = 0; i < computationTime.length; i++) {
			mean = mean + computationTime[i];
		}
		mean = mean / computationTime.length;

		for (int i = 0; i < computationTime.length; i++) {
			standardDeviation = standardDeviation + Math.pow(computationTime[i] - mean, 2);
		}

		standardDeviation = standardDeviation / (computationTime.length - 1);

		return Math.sqrt(standardDeviation);
	}

	private static int getExitTask(int[][] communicationMatrix, int tasks) {
		for (int i = 0; i < tasks; i++) {
			int counter = 0;
			for (int j = 0; j < tasks; j++) {
				if (communicationMatrix[i][j] != 0)
					counter++;
			}
			if (counter == 0)
				return i;
		}

		return 0;
	}

}
