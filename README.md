# Galaxia LogToStats

Library that reads one or more webserver log files, and writes a php array with statistics for each day.

It is meant to be run from the command line or a cron script, and also from web scripts, for example to show updated today's statistics.
It keeps a track of the last line read so it can skip already processed lines in the same log file.
Designed be used on daily logs, in a sequential order.

- The log format is parsed by regex – common log format with optional `speed` and `cache` appended is the default and can be changed.
- Uses WhichBrowser for browser data extraction from user agents.
- Uses GeoIP for geolocation. Does not come with ane GeoIP database, but the file comes included in some linux distributions.
- By default the output is gzipped, to save space and avoid wasting opcache memory, but it can be changed.
- The output php array does not contain personal information such as `IP` or `user agents`.
- Uses own cache files for browser, country and user statistics,


# GDPR compliance

Web server logs are personal information and should be kept private and only stored for a reasonable time and be deleted afterwards. This process should be automated, for example using a utility like `logrotate`.

The resulting statistics from LogToStats do not contain `IPs` or `user agents`, and geolocation is done by country.

The cache files do contain a hashe of the `IP` and `user agent` and it's not a cryptographic hash. When LogToStats is run, cache entries older than 7 days are removed, so be sure to automate LogToStats with something like `cron`.
