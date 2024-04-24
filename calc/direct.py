import argparse

EMR_S_VCPU_COST_HOUR = 0.00747
EMR_S_VMEM_COST_HOUR = 0.00082
FLINT_OCU_COST_HOUR = 0.24

class Task:
    def __init__(self, task_duration):
        self.task_duration = task_duration
        self.runtime = 0

    def run(self):
        self.runtime += 1

    def is_finished(self):
        return self.runtime == self.task_duration


class Core:
    def __init__(self):
        self.current_task = None
        self.runtime = 0

    def run(self):
        if self.current_task is not None:
            self.current_task.run()
        self.runtime += 1

    def assign_task(self, task):
        self.current_task = task

    def is_free(self):
        return self.current_task is None or self.current_task.is_finished()
    
    def roundUpRuntime(self):
        if self.runtime <= 60:
            return 60
        else:
            return self.runtime


class Allocator:
    def __init__(self):
        self.allocate_delay = 40
        self.timestamp = 0
        self.desired = 0
        self.allocated = False

    def allocate(self, desired):
        self.desired = desired

    def run(self):
        self.timestamp += 1

    def get(self):
        if self.allocated:
            return 0
        if self.timestamp >= self.allocate_delay:
            self.timestamp = 0  # Reset timestamp after allocation
            self.allocated = True
            return self.desired
        else:
            return 0


def simulator(data_scanned_in_gb=100, init_cores=12, max_cores=120):
    tasks = data_scanned_in_gb * 1024 // 128
    allocator = Allocator()
    core_list = [Core() for _ in range(init_cores)]
    total_runtime = 0

    if tasks > init_cores:
        allocator.allocate(min(max_cores - init_cores, tasks - init_cores))

    while tasks > 0:
        total_runtime += 1

        # Handle dynamic resource allocation
        dra_cores = allocator.get()
        if dra_cores > 0:
          print(f"DRA cores: {dra_cores}")
        for _ in range(dra_cores):
            core_list.append(Core())

        # Assign tasks to free cores
        for core in core_list:
            if core.is_free() and tasks > 0:
                core.assign_task(Task(5))

        # Run the cores
        for core in core_list:
            core.run()

        allocator.run()

        for core in core_list:
            if core.is_free() and tasks > 0:
                tasks -= 1
    
    print(f"Total runtime: {total_runtime} seconds")
    total_runtime = sum(core.runtime for core in core_list)
    print(f"Total runtime of all cores: {total_runtime} seconds")
    total_billedTime = sum(core.roundUpRuntime() for core in core_list)
    print(f"Total billed time of all cores: {total_billedTime} seconds")

    return total_billedTime


def direct_query_cost(total_billedTime):
    cost = total_billedTime * (EMR_S_VCPU_COST_HOUR + 4 * EMR_S_VMEM_COST_HOUR) / 3600
    return cost

def direct_query_bill(total_billedTime):
    price = total_billedTime * FLINT_OCU_COST_HOUR / 2 / 3600
    return price

def main():
    parser = argparse.ArgumentParser(description="Run a core-task simulation with dynamic allocation.")
    parser.add_argument("--data_scanned_in_gb", type=int, default=100,
                        help="Amount of data in GB to be processed in the simulation.")
    args = parser.parse_args()

    total_billedTime = simulator(data_scanned_in_gb=args.data_scanned_in_gb)
    print(f"Cost: {direct_query_cost(total_billedTime)}$")
    print(f"Price: {direct_query_bill(total_billedTime)}$")


if __name__ == "__main__":
    main()
