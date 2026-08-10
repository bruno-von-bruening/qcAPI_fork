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

# Problems
Failures when running multiple workers (likely do to worker timeout). Have to trace the current.
It's probably my timeout restriction which is applied at multiple points!


# Next

# Open-Shell
Currently all wave function are populated according to input (e.g. RHF for all)
Would need to change either the population procedure ore allow for an auto option that assigns automatically (but then
still a problem with UHF vs ROHF)

Major problem: Currently the uid is inchikey  (which only considers charge but not multiplicity!!)
Should I include multiplicity in conformation instead?

WORKAROUND:
* change multiplicity in compound
* change protocol hash.
* Compute and see error and supposed protocol hash
* Change protocol hash and id!
* Remove molecular_polarizabilitity
* Recompute



