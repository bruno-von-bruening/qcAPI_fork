# [DEV-1.0.0] Distributed Polarisabilities and Storage management

Implementd distributed Polarisabilities run (via camcasp) and improved the file managment.

## Distributed Polarisabilities
Run polarisabilities via custom run_camcasp package

REMAINING:
*Add GRAC possibility to the PBEO

## Storage managment
Push files from worker to central storage at server. Both files and whole run directory

REMAINING:
* Work on efficiency (do not store to much)
* Check file deletion system (if entry gets dropped all dependant files should be deleted)
* Allow for mutliple file download at once to limit traffic


# v0.2.0 molecular polarisabilities through finite field, new run script, some restructuring
* Implemented polarisability run (currently in a crude developmental version)
* Reorganized run script (single script with keyword for operation)
* Restructured the internals. Ideally it would be super abstract with few assumptions beyond a generic class style and
  some other cannonic fields.





