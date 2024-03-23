package com.onedayoffer.taskdistribution.services;

import com.onedayoffer.taskdistribution.DTO.EmployeeDTO;
import com.onedayoffer.taskdistribution.DTO.TaskDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TaskDistributorImpl implements TaskDistributor {
    public static final int MINUTES_PER_SEVEN_HOURS = 420;

    @Override
    public void distribute(List<EmployeeDTO> employees, List<TaskDTO> tasks) {
        validateInputData(employees, tasks);

        log.info("Start distribute() with {} employees and {} tasks", employees.size(), tasks.size());
        Map<Integer, List<TaskDTO>> tasksByPriority = tasks.stream()
                .collect(Collectors.groupingBy(TaskDTO::getPriority));

        tasksByPriority.values().forEach(taskList ->
                taskList.sort(Comparator.comparingInt(TaskDTO::getLeadTime)));

        AtomicInteger addingTaskCount = new AtomicInteger();

        for (int priority = 1; priority <= 10; priority++) {
            List<TaskDTO> currentPriorityTasks = tasksByPriority.getOrDefault(priority, new ArrayList<>());

            for (TaskDTO task : currentPriorityTasks) {
                employees.stream()
                        .filter(employee -> (employee.getTotalLeadTime() + task.getLeadTime()) <= MINUTES_PER_SEVEN_HOURS)
                        .findFirst()
                        .ifPresent(freeEmployee -> {
                            freeEmployee.getTasks().add(task);
                            log.info("Employee with fio: {} add task with id: {} and priority: {}",
                                    freeEmployee.getFio(), task.getId(), task.getPriority());
                            addingTaskCount.getAndIncrement();
                        });

            }
        }

        int unallocatedTasksCount = tasks.size() - addingTaskCount.get();
        if (unallocatedTasksCount != 0) {
            log.warn("{} task(s) were not distributed among employees!", unallocatedTasksCount);
            return;
        }
        log.info("All tasks were distributed among employees");
    }

    private void validateInputData(List<EmployeeDTO> employees, List<TaskDTO> tasks) {
        if (employees == null || employees.isEmpty()) {
            log.warn("Employees list is empty or null!");
            throw new IllegalArgumentException("Employees list is empty or null");
        }
        if (tasks == null || tasks.isEmpty()) {
            log.warn("Tasks list is empty or null!");
            throw new IllegalArgumentException("Tasks list is empty or null");
        }
    }
}
