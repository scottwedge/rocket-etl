# rocket-etl
An ETL framework customized for use with a) CKAN and b) the specific needs and uses of the [Western Pennsylvania Regional Data Center](https://www.wprdc.org) open-data portal. This uses the [wprdc-etl framework](https://github.com/WPRDC/wprdc-etl/) for core ETL functionality.

# File structure

`launchpad.py` is located in the root directory for this repository. In the `engine` subdirectory can be found the `wprdc_etl` directory, `notify.py` for sending Slack notifications, and `etl_util.py` which provides a lot of extra functionality for configuring and running pipelines, setting and using metadata, querying CKAN about existing resources, and automatically setting up CKAN resource views.

In the `engine/payload` subdirectory can be found an arbitrary number of directories, named based on the data source/publisher. This provides a namespacing for organizational purposes to help avoid collisions. (This namespacing is also replicated in the `source_files` directory, which is a subdirectory of the directory where `launchpad.py` resides.) Each payload subdirectory can contain an arbitrary number of scripts, and each script can describe multiple ETL jobs (or steps in jobs). 

# Use
To run all the jobs in a given module (say `pgh/dpw_smart_trash.py`), run this:
`> python launchpad.py engine/payload/pgh/smart_trash.py`

Since launchpad knows where the payloads can be, you can also use these variants:
`> python launchpad.py payload/pgh/smart_trash.py`

`> python launchpad.py pgh/smart_trash.py`

A number of options can be set using command-line parameters:

Force the use of local files (rather than fetching them via FTP) housed in the appropriate `source_files` subdirectory:
`> python launchpad.py pgh/smart_trash.py local`

Run the script in test mode, which means that rather than writing the results to the production version of the CKAN package described in the `jobs` list in the script, the data will be written to a default private CKAN package:
`> python launchpad.py pgh/smart_trash.py test`

The `PRODUCTION` boolean in `engine/parameters/local_parameters.py` decides whether output defaults to the production package or the test package.
Thus running with the `test` command-line argument is only necessary when the `PRODUCTION` variable is True. When `PRODUCTION` is False, sending data to the production package requires using the `production` command-line parameter, like this:
`> python launchpad.py pgh/smart_trash.py production`

Run the script without sending any Slack alerts:
`> python launchpad.py pgh/smart_trash.py mute`

Log output to a default log location (also namespaced by payload directory names):
`> python launchpad.py pgh/smart_trash.py log`
This one is intended for production use.

Clear the CKAN resource before upserting data (necessary for instance when he fields or primary keys have changed):
`> python launchpad.py pgh/smart_trash.py clear_first`

Reverse the notification-sending behavior to only send a notification if the source file is found:
`> python launchpad.py pgh/smart_trash.py wake_me_when_found`

Any other command-line parameters will be interpreted as candidate job codes (which are the filename of the source file as configured in the job WITHOUT the extension).  So if `pgh_smart_trash.py` contained three jobs and two of them had source files named `oscar.csv` and `grouch.xlsx`, running

`> python launchpad.py pgh/smart_trash.py oscar grouch`

would run only the `oscar` and `grouch` jobs.

The command-line parameters can be specified in any order, so a typical test-run of an ETL job could be 
`> python launchpad.py pgh/smart_trash.py local mute test`
or 
`> python launchpad.py pgh/smart_trash.py test mute local`


To search for all jobs in the payload directory and test them all (directing upserts to a default test package on the CKAN server), run this:
`> python launchpad.py test_all`
This is useful for making sure that nothing breaks after you modify the ETL framework.
