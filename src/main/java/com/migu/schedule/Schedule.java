package com.migu.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule
{
    // 任务挂起队列
    public List<TaskInfo> freeTaskInfoList;
    
    // 节点下的任务信息
    public Map<Integer, List<TaskInfo>> nodeAndTasks;
    
    int nodeId = 0;
    
    public int init()
    {
        freeTaskInfoList = new LinkedList<TaskInfo>();
        
        nodeAndTasks = new HashMap<Integer, List<TaskInfo>>();
        
        return ReturnCodeKeys.E001;
    }
    
    public int registerNode(int nodeId)
    {
        // 服务节点编号非法
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        // 服务节点已注册
        if (null != nodeAndTasks.get(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        
        List<TaskInfo> taskInfoList = new LinkedList<TaskInfo>();
        // 将新节点注册，并初始化节点下的任务
        nodeAndTasks.put(nodeId, taskInfoList);
        
        return ReturnCodeKeys.E003;
    }
    
    public int unregisterNode(int nodeId)
    {
        // 服务节点编号非法
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        // 服务节点不存在
        if (null == nodeAndTasks.get(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        List<TaskInfo> taskInfoList = nodeAndTasks.get(nodeId);
        // 如果该节点里有处理中的任务则把任务放到挂起列表中
        if (!isEmpty(taskInfoList))
        {
            freeTaskInfoList.addAll(taskInfoList);
        }
        // 删除节点
        nodeAndTasks.remove(nodeId);
        
        return ReturnCodeKeys.E006;
    }
    
    public int addTask(int taskId, int consumption)
    {
        // 任务编号非法
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        for (TaskInfo taskinfo : freeTaskInfoList)
        {
            if (taskId == taskinfo.getTaskId())
            {
                return ReturnCodeKeys.E010;
            }
        }
        TaskInfo newTask = new TaskInfo();
        newTask.setTaskId(taskId);
        newTask.setConsumption(consumption);
        freeTaskInfoList.add(newTask);
        return ReturnCodeKeys.E008;
    }
    
    public int deleteTask(int taskId)
    {
        // 任务编号非法
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        boolean find = false;
        TaskInfo taskinfotemp = new TaskInfo();
        for (TaskInfo taskinfo : freeTaskInfoList)
        {
            if (taskId == taskinfo.getTaskId())
            {
                find = true;
                taskinfotemp = taskinfo;
                break;
            }
        }
        // 找不到该任务
        if (!find)
        {
            return ReturnCodeKeys.E012;
        }
        int index = freeTaskInfoList.indexOf(taskinfotemp);
        freeTaskInfoList.remove(index);
        return ReturnCodeKeys.E011;
    }
    
    public int scheduleTask(int threshold)
    {
        // 调度阈值非法
        if (threshold <= 0)
        {
            return ReturnCodeKeys.E002;
        }
        if (!isEmpty(freeTaskInfoList))
        {
            List<TaskInfo> freeTaskInfoListtemp = new LinkedList<TaskInfo>();
            freeTaskInfoListtemp.addAll(freeTaskInfoList);
            for (TaskInfo taskInfo : freeTaskInfoListtemp)
            {
                int freeNodeId = findTheFreeNode();
                addTaskToNode(freeNodeId, taskInfo);
            }
        }
        return ReturnCodeKeys.E013;
    }
    
    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        Map<Integer, TaskInfo> taskMap = new HashMap<Integer, TaskInfo>();
        
        Iterator<Entry<Integer, List<TaskInfo>>> ite = nodeAndTasks.entrySet().iterator();
        // 遍历map
        while (ite.hasNext())
        {
            Entry<Integer, List<TaskInfo>> entry = ite.next();
            int nodeId = entry.getKey();
            List<TaskInfo> taskInfoList = entry.getValue();
            for (TaskInfo taskInfo : taskInfoList)
            {
                taskMap.put(taskInfo.getTaskId(), taskInfo);
            }
        }
        tasks.addAll(taskMap.values());
//        TreeSet<Integer> taskIdSet = new TreeSet<Integer>();
//        taskIdSet.addAll(taskMap.keySet());
        return ReturnCodeKeys.E015;
    }
    
    
    public static boolean isEmpty(Map map)
    {
        if ((null == map) || (map.isEmpty()))
        {
            return true;
        }
        
        return false;
    }
    
    public int findTheFreeNode()
    {
        Iterator<Entry<Integer, List<TaskInfo>>> ite = nodeAndTasks.entrySet().iterator();
        int minConsumption = Integer.MAX_VALUE;
        int mostFreeNodeId = 0;
        // 遍历map
        while (ite.hasNext())
        {
            Entry<Integer, List<TaskInfo>> entry = ite.next();
            int nodeId = entry.getKey();
            List<TaskInfo> taskInfoList = entry.getValue();
            if(!isEmpty(taskInfoList))
            {
                int consumptionTotal = 0;
                for(TaskInfo taskInfo :taskInfoList)                   
                {
                    consumptionTotal = consumptionTotal + taskInfo.getConsumption();
                }
                if(consumptionTotal < minConsumption)
                {
                    minConsumption = consumptionTotal;
                    mostFreeNodeId = nodeId;
                }     
            }
            else
            {
                mostFreeNodeId = nodeId;
                return mostFreeNodeId;
            }
        }
        return mostFreeNodeId;
    }
    
    public void addTaskToNode(int nodeId, TaskInfo taskInfo)
    {
        // 没有该节点，不做任务操作
        if(null == nodeAndTasks.get(nodeId))
        {
            return;
        }
        List<TaskInfo> TaskInfoList = nodeAndTasks.get(nodeId);
        TaskInfoList.add(taskInfo);
        taskInfo.setNodeId(nodeId);
        freeTaskInfoList.remove(taskInfo);
        return;
    }
    
    public static boolean isEmpty(List<? extends Object> lst)
    {
        if ((null == lst) || (lst.isEmpty()))
        {
            return true;
        }
        return false;
    }
      
}
