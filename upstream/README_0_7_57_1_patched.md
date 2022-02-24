# Patching 0.7.57.1 release to enable table export feature

## Motivation

This patched release provides a new `export_tables` binary to facilitate exporting an existing zlib compressed [database clone](https://dev.overpass-api.de/clone/), and make it available for easy importing in newer Overpass releases running on incompatible database file formats.

It should be used along with the `import_tables` binary included in 0.7.59_mmd to recreate the database using lz4 compression in 0.7.59_mmd file format.

Cereal in binary archive format is used for (de)serialization of database files across incompatible versions.

Sample script for database file conversion:

```
#!/bin/bash
for a in `seq 1 45` ;
do
  sem -j-4 "
      ./export_tables /opt/clone/ $a | ./import_tables /opt/new_db/ $a
      "
done
sem --wait
```

where:
* export_tables: binary provided by patched 0.7.57.1
* import_tables: binary provided by 0.7.59_mmd

## Building

* Untar archive: `tar xvfz osm-3s_v0.7.57.1.tar.gz`
* Change to new folder: `cd osm-3s_v0.7.57.1`
* Apply patch: `patch -p1 < ../0001-Fixup-0.7.57.1.patch`
* Build export_tables binary: `make V=0 -j3 bin/export_tables`


## Original file location

* https://dev.overpass-api.de/releases/osm-3s_v0.7.57.1.tar.gz

## List of changes

* Fix Change_Entry lower than comparison operator (https://github.com/mmd-osm/Overpass-API/commit/b4bd63990bf840562efb8137aba34879e3785514)
* Added export_tables.cc in overpass-api/osm-backend directory to export bin/map files in 0.7.57 format
* Makefile.am: added settings to generate bin/export_tables

## Additional dependencies

* libcereal-dev


