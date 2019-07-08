* SORLoader

This repo is for a Node.js application to handle data imports into an SoR object store

Usage:
```node loader.js /path/to/import_file.xml```

The loader currenrly supports registration, HR, and Instructor import data. It will determine
what is being loaded based on the XML content

All SoR data is loaded into a single table. Entries are unique for a given SoR/SFUID combo.
I.e only one record can exist for a given system-of-record for a given SFUID, but the same
SFUID may appear multiple times if it appears in multiple import files. 

Each record consists of a very small number of columns and a JSONB column to hold the entire
user object. It's designed to be used with Postgres's built-in support for JSONB. Testing
has shown that no indexes are necessary to produce fast search results. However,
the data in the SoR Table is not meant for direct consumption. It should be used to feed
downstream systems of record, such as Grouper or an identity registry.

Refer to the `psqljson-cheatsheet.txt` file for examples of PSQL queries against the
data for that purpose.
