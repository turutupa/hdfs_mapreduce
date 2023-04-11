# ​​Project Retrospective

## How many extra days did you use for the project?

We completed the project on November 27.

## Given the same goals, how would you complete the project differently if you didn’t have any restrictions imposed by the instructor? This could involve using a particular library, programming language, etc. Be sure to provide sufficient detail to justify your response.

In terms of programming language, we would not have chosen a different one. Go was a great language to use for this project. It provided a simple and powerful standard library and the goroutines made concurrency easy. However, one thing we may have done differently was to use an external data structures library that provided a priority queue / min heap. This would have helped in developing more efficient sorting algorithms for MapReduce.

## Let’s imagine that your next project was to improve and extend P2. What are the features/functionality you would add, use cases you would support, etc? Are there any weaknesses in your current implementation that you would like to improve upon? This should include at least three areas you would improve/extend.

The biggest weakness in the current implementation that we would improve upon is its efficiency. If the next project was to improve P2, we would make it more efficient by implementing a more efficient merge sort algorithm. The current implementation uses an inefficient merge sort algorithm which runs in O(n^2) time. The use of an external library which provides a heap data structure would help in implementing a more efficient merge sort.

Another area we would improve upon would be in load balancing. Our current implementation randomly chooses mappers based on which storage nodes have chunks of the file which we want to do a computation on. However, this can make things inefficient if one storage node is randomly chosen to run two mappers. By implementing a better load balancing algorithm, we could have a more efficient system and cut down on computation time.

A feature that we would implement in an extension of P2 would be more fine-grained progress reports when running a computation. In our current implementation, the system logs when mappers and reducers are assigned, when a mapper begins and completes, and when a reducer begins and completes. However, while the user is waiting for their computation, they are unsure how far along the progress is. By implementing more thorough logging and progress reports, the user can have a better understanding of how long their computation will take to complete.

## Give a rough estimate of how long you spent completing this assignment. Additionally, what part of the assignment took the most time?

Ahmed spent approximately 45 hours completing this assignment. What took me a lot of time at the beginning of the project was getting acquainted with the codebase, how the DFS worked, and figuring out how to extend it. This is because we built on top of Alberto’s P1 implementation for this project. The part of the assignment that took me the most time was implementing the shuffle and merge sort phase. I ran into problems with communication between the different software components running on a specific storage node and sending data between different storage nodes.

Alberto: spent over 60 hours for sure. Way more I think. Specially due to some weird bugs that had me spent entire days searching for. Communication between components was definitely the part that took more time both to build and to debug. External sort was surprinsingly not so bad. We thought it would be a bad headache but it wasn't so bad. Also not the most efficient I guess. The most frustrating part was the we spent so many hours, and still missed some race condition or someting that made the overall system not function too efficiently. Maybe it was due running the plugin as an `exec` job and grabbing the outoput from `stdout`. Partitioning was also tricky. The implementation wasn't hard, but sometimes if the mapreduce job's output wasn't split properly (key,value) the partitioning wouldn't work properly.

## What did you learn from completing this project? Is there anything you would change about the project?

We learned a lot from completing this project. We developed a much deeper understanding of the inner workings of MapReduce by implementing our own system. One thing I learned about is external sort algorithms. When dealing with a massive amount of data that needs to be sorted, it can’t all be read into memory and sorted with traditional sorting algorithms. This was my first exposure to and experience with external sort.

We would not change anything about the project spec and requirements. We believe they provided a good scope to focus on the important aspects of MapReduce and we liked the requirement of implementing word count as well as a data analysis job of our choosing because it gave us an opportunity to look at applications of MapReduce.

## If you worked as a group, how did you divide the workload? What went well with your team, and what did not go well?

Initially we did some pair programming and worked on the project together. This worked well because we were building on top of Alberto’s codebase and it gave Ahmed an opportunity to understand the system and have it explained by Alberto. We did this by using collaborative coding tools in GoLand and later VS Code.
Later we decided to split up tasks with Alberto primarily focusing on the map phase and Ahmed primarily working on the reduce phase. This did not work as well because it was difficult to test the reduce phase without the map phase having been completed. We also should have discussed how to integrate the two systems better before beginning. Later on in the project, we listed tasks that needed to be completed and assigned them. We also began creating pull requests and reviewing each other's code before merging into main. It would have helped if we started doing this earlier because it gives each other an opportunity to understand the other persons code and the system as a whole better.
