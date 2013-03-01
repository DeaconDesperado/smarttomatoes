## An exercise in collaborative intelligence

A simple POC using the Rotten Tomatoes API to compare Top Movie Critics and their commonality to one another.

Uses the Pearson Similarity Metric:

![image](http://snips.deacondesperado.com/rtapi/pear.gif 'Pearson Metric')


Where x and y = the set of two individual's comparative ratings for titles.

The service itself is wrapped inside a Tornado webserver.  A separate thread of execution periodically rebuilds the index and loads in into memory, keeping read perfomance consistent regardless of volume of requests.

The pearson calculation is done in this background task and cached by critic name.