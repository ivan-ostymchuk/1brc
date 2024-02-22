# 1BRC - One Billion Rows Challenge
The challenge ideated by Gunnar Morling was to calculate the mean, max, and min temperature   
of each city in a text file of 1 billion rows. One row of data has the following schema:
cityName;temperature(float with precision of 1)
For example: London;14.5

The challenge was meant to be done only in Java and with standard library only (No external dependencies).
I decided to try it in Golang (standard library only) since it’s the language I’m using the most at the moment.
I took it as a learning opportunity, to try and get more knowledge on optimizing both writing and processing data in Golang.
It was also the perfect opportunity to play and experiment an awful lot with Goroutines and channels and the different usage patterns of those.

I didn’t want to download Java just to generate the file so I rewrote the file generation code in Golang.
Surprisingly It took longer than I expected due to some weird bugs from the generation of random numbers in goroutines.
It was also a great exercise to try to optimize the file generation, finding strategies to write data to disk as fast as possible.

Usually, when approaching these kinds of optimization problems you should start from a baseline implementation and iteratively improve it.
It wasn’t an option for me because my laptop is old with limited RAM and CPU. The baseline approach would have taken days to execute.
I had to parallelize the execution from the very start and iteratively improve it. The steps that provided a boost in the execution time:
-	Reading the file in bigger buffers and sending the buffered data to different workers for processing. Collect results at the end.
-	Pre-size all the maps used to store the calculations to avoid continuous resizing.
-	Iterating directly over the bytes of each buffer instead of transforming the data into strings.
-	Using an unsafe string for the city name (from bytes to string without copy) to perform the lookup on the map
-	Transforming temperature value directly from bytes to float instead of doing bytes -> string -> float.
I tried also to memory map the file, but it didn't give a noticeable increase in performance.

Then I wanted to test the solution on a decent machine so used a VM on GCP with the following characteristics:
- Machine type: t2d-standard-8
- Processor: AMD Milan
- Ram: 32 GB
- vCPU: 8

On that hardware, my solution took 6.6 seconds to execute.

Then I tried also the Swiss Map, which is a more performant implementation than the Golang standard map.
It improved the result by 0.2 seconds. However, it was not part of the standard library so I discarded it.
I learned an incredible amount of things by doing this challenge. If you want to get more familiarity with any programming language it would be a great exercise.
Here are the things you would play with:
-	Optimization of reading, processing, and writing files.
-	All the different patterns of using goroutines and channels.
-	Mutexes and how to avoid them for more speed.
-	Concurrent maps.
-	Swiss maps.
-	Memory-mapped files.
