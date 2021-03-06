# Site Exec

_Note that this is still very much under construction._ 

This is a little testbed for a network of distributed Sites (one per node) working together to drive a distributed computation workflow. Two activities are of interest:
1. **Planning** creates and distributed plans given a scenario, where a _plan_ is characterized by a site-local set of instructions, and _scenario_ directs data flow through compute steps to produce a set of outputs. If done correctly, the combined efforts of sites working on their local plans emerges as the distributed system executing the scenario. See `plan` in `src/planning.rs`.

1. **Plan execution** is the process by which a site works to empty its set of planned instructions by _completing_ each of them. Instructions have post- and pre-conditions that access a local store of data assets, and send messages to other sites. See `Site::execute` in `src/site.rs`.

See `amy_bob_cho.rs` in `src/scenario.rs` for an example of a particular scenario.