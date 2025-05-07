
SnapshotPrepender is the utility to be used in production. 

DemoApplication and pretty much everything else is just for testing. 

Besides robustifying and addressing the TODOs, some potential enhancements: 
* Add a "snapshot complete" signal (maybe just a data row like `{"snapshot":"complete"}` or somesuch) 
* Delete updates that arrived earlier than the snapshot update.
* It should work with a regular conflation buffer if desired, but should there be a built-in special-purpose
  conflation buffer that operates only up to the point when the updates go hot? 
