from simulation import WorkItem, run_simulation, print_simulation_results

def main():
    # Create sample work items
    items = [
        WorkItem("task1", 10),
        WorkItem("task2", 8),
        WorkItem("task3", 15),
        WorkItem("task4", 6),
        WorkItem("task5", 12),
        WorkItem("task6", 4),
        WorkItem("task7", 9),
        WorkItem("task8", 7),
        WorkItem("task9", 5),
        WorkItem("task10", 11),
        WorkItem("task11", 3),
        WorkItem("task12", 14),
    ]
    
    # Run simulation with 3 threads
    num_threads = 3
    # Use 1.0 as the base time unit (so size directly corresponds to time units)
    processing_time_unit = 1.0
    
    print("Starting simulation...")
    print("\nWork items (sorted by size):")
    sorted_items = sorted(items, key=lambda x: x.size, reverse=True)
    for item in sorted_items:
        print(f"  - Key: {item.key}, Size: {item.size}")
    
    threads = run_simulation(items, num_threads, processing_time_unit)
    print_simulation_results(threads)

if __name__ == "__main__":
    main() 