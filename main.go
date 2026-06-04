package main

import (
	"log"
	"time"

	scheduler "Scheduler/code"
)

func main() {
	// Configure the log package to only show the time (HH:MM:SS)
	// instead of the default date + time.
	log.SetFlags(log.Ltime)

	taskScheduler := scheduler.New()
	taskScheduler.Start()
	defer taskScheduler.Stop()

	startTime := time.Now().Unix()
	log.Println("Simulation started.")
	log.Println()

	// Helper to turn absolute unix timestamps into readable strings for "Target: HH:MM:SS"
	fmtTime := func(ts int64) string {
		return time.Unix(ts, 0).Format("15:04:05")
	}

	// --- 1. Schedule 4 tasks with 5-second steps ---
	log.Println("--- Scheduling Initial 4 Tasks ---")

	taskScheduler.Schedule("Task1", startTime+5, func() {
		log.Printf("- Executed: Task1 (Target: %s)", fmtTime(startTime+5))
	})

	taskScheduler.Schedule("Task2", startTime+10, func() {
		log.Printf("- Executed: Task2 (Target: %s)", fmtTime(startTime+10))
	})

	taskScheduler.Schedule("Task3", startTime+15, func() {
		log.Printf("- Executed: Task3 (Target: %s) <-- SHOULD NOT RUN", fmtTime(startTime+15))
	})

	taskScheduler.Schedule("Task4", startTime+20, func() {
		log.Printf("- Executed: Task4 (Target: %s)", fmtTime(startTime+20))
	})

	// --- 2. Let the simulation run for 7 seconds ---
	time.Sleep(7 * time.Second)

	// --- 3. Simulate Cancellation of Task3 ---
	log.Println("--- Triggering Cancellation ---")
	if taskScheduler.Cancel("Task3") {
		log.Println("- Task3 was successfully cancelled before execution!")
	} else {
		log.Println("- Failed to cancel Task3.")
	}

	// --- 4. Add 1 more task set 20 seconds into the future from right now ---
	now := time.Now().Unix()
	log.Println("--- Scheduling Late Task (+20s from now) ---")
	log.Println()

	taskScheduler.Schedule("Task_Late_Bonus", now+20, func() {
		log.Printf("- Executed: Task_Late_Bonus (Target: %s)", fmtTime(now+20))
	})

	// --- 5. Wait out the remaining timeline ---
	log.Println("- Main thread waiting for remaining tasks to complete...")
	time.Sleep(22 * time.Second)

	log.Println("- Simulation timeline finished.")
}
