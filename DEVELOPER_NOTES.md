# CI tests
One tests that checks if after pushing the molecular polarizabilities could be produced when installing through conda
environment file. Currently run by hand (could implement it on commit, which requires a bit more work to make sure the
new environment (not the old is checked)).

# Client
```python
    main_record, run_information, other records = compute_function(...)
```
Then process:
```python

```

# Next
What do I need next?

Get my script ready for production.
* 

# TODO 
- Client 
  - [ ] Dump yaml file 
  - [ ] What to do when no cleaning file is found
  - [ ] save load_user, load_system, load_total_avr
  - [ ] Push abort process from client (if interrupted send kill signal!)
  - [ ] give time stamp to user marker
  - [ ] folder in which every client runs in and drops information
  - [ ] read resources from file
- ORM
  - [ ] Individual wfn table
  - [ ] Change WFN status
  - [ ] Unique constraint in molecular multipoles
- Server
  - [ ] track and display timings somewhere (figure out response bottle-necks)
  - [ ] double counting in some populations (e.g. wfn)
