# ProjectKit

A project management framework that aims to make reproducible experiments easy to manage. Very much work in progress at this time.

Example usage:

```
python2 -m projectkit.project init # initialize an empty project
# At this point, fill in data generation and run scripts
python2 -m projectkit.project gen data/mnist.tgz mnist_data # Generate data using the generation script, with data/mnist.tgz as input.
# The generated dataset will have the prefix 'mnist_data'.
python2 -m projectkit.project run my_first_experiment exp1 mnist_data # Run the experiment 'my_first_experiment',
#naming the run 'exp1', and use the latest version of the data named 'mnist_data'.

python2 -m projectkit.project find model exp1 # Find the latest version of the 'exp1' run, such as to read its logs or to load it
python2 -m projectkit.project find data mnist_data # Find the latest version of the 'mnist_data' dataset
```

## Features
    * Empty project generation
    * SQLite experiments database
    * Git-based automatic code versioning
    * Simple experiment query and cleanup
    * Experimental parameter saving
    * Programmatic access for simple functions (see: `latest`)
    * Data generation, logging and experimental runs management

## TODO
    * More active project dispatch management (clusters, etc.)
    * Cleanup
    * More commands
    * More programmatic access to functions
    * Test experiments management
    * Much more!
