# 1BRC - One Billion Rows Challenge
The challenge ideated by Gunnar Morling was to calculate the mean, max and min temperature   
of each city in a text file of 1 billion rows. One row of data has the following schema:
cityName;temperature(float with precision of 1)
For example: London;14.5

The challenge was meant to be done only in Java and with standard library only (No external dependencies).
But since I was doing many things in Golang at the time, I decided to try it in Golang (standard library only).
I took it as a learning opportunity, to try and get more knowledge on optimizing both writing and processing data in Golang.
It was also the perfect opportunity to play and experiment an awful lot with Goroutines and channels and the
different usage patterns of those.

The bytesToFloat function I copied it from Eugene Huang (https://github.com/elh/1brc-go) as it was a clever solution.

Since I have an old laptop with not much RAM and not a powerful processor I decided to skip completely
the baseline solutions, as the execution would have taken days to complete.
I started directly with the following steps:
1. Read file with bufio in bigger buffers, send the buffered data to different workers for processing. Collect results at the end.
2. Pre-size all the maps to avoid continuous resizing.
3. Avoid converting to string each buffer (then split and process). Therefore, iterating over bytes, converting
   directly to float (instead of bytes -> string -> float) and using unsafe string for the city name (from bytes to string without copy) to perform the lookup on the map.

I tried also to memory map the file, but it didn't give a noticeable increase in performance.

Then I wanted to test the solution on a decent machine so used a VM on GCP with the following characteristics:
- Machine type: t2d-standard-8
- Processor: AMD Milan
- Ram: 32 GB
- vCPU: 8

On that hardware my solution took 6.6 seconds to execute.
