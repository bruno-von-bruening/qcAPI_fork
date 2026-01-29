
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
- [ ] Save FCHK_file (passing through http)
- [ ] Dump yaml objects in directory
- [ ] Molecular multipoles

I do not understand the whole logic around workers (should I change that?)


# Versioning (for development)
Use ```setuptools-scm``` for sub-versioning. Assigns commit has allowing for easy installing of environment through pip via github branch.

Use script to print versions of used pacakges into yaml files.
-> put that into qcp_global_utils (provide list of packages as input, get rest as output)

Use script to create pip installs for the precise packages needed.

## Procedure
First gereneate a test environment that loads the last commits used 

When testing, run with a test environment that uses the dependencies as loaded through pip. 
This makes sure this package will run reproducible if loaded from git.
Furthermore, the individual git packages do not have to be downloaded individually.

# New commits
Make as many tests as possible. Minimally test the function that the new commit will be used for.

1. First test with development environment (local pip installs)
2. Drop this local environment to yaml file (check dirty commits that will raise warning)
3. Install this new environment (can be done through qcp_export), install local package, and activate
4. Run the same test (if unit test then check against reference)
5. Add the changes from the new conda env to the install/*.yaml file and commit everything
6. Ready to just install this package now!