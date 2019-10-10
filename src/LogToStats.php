<?php
/* Copyright 2019 Ino Detelić

 - Licensed under the EUPL, Version 1.2 only (the "Licence");
 - You may not use this work except in compliance with the Licence.

 - You may obtain a copy of the Licence at: https://joinup.ec.europa.eu/collection/eupl/eupl-text-11-12

 - Unless required by applicable law or agreed to in writing, software distributed
   under the Licence is distributed on an "AS IS" basis,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the Licence for the specific language governing permissions and limitations under the Licence.
*/

use WhichBrowser\Parser;

namespace Galaxia;


class LogToStats {

    public $dirLog   = 'var/logs/';
    public $dirStats = 'var/stats/';
    public $dirCache = 'var/cache/';
    public $filePathGeoIpDB = 'GeoIP.dat';

    public const OUTPUT_SILENT   = 0;
    public const OUTPUT_ERRORS   = 1;
    public const OUTPUT_COMPLETE = 2;
    public const OUTPUT_INFO     = 3;
    public const OUTPUT_DEBUG    = 4;

    public $cli       = true;
    public $verbosity = self::OUTPUT_COMPLETE;
    public $output    = [];

    public $logRegexPattern =
        '~' .
        '^(?<host>\S+) ' .
        '(?<ip>\S+) - ' .
        '\[(?<datetime>[^\]]*)] "' .
        '(?<method>\S+) ' .
        '(?<url>\S+) ' .
        '(?<protocol>[^"]*)" ' .
        '(?<status>\d{3}) ' .
        '(?<bytes>\d+) "' .
        '(?<referer>[^"]*)" "' .
        '(?<ua>[^"]*)" ?' .
        '(?<speed>\d+\.\d+)? ?' .
        '(?<cache>.+?)?$' .
        '~';

    public $compressStreamPrefix = 'compress.zlib://';
    public $compressStreamSuffix = '.gz';

    private $countries = [];
    private $browsers  = [];
    private $retention = [];

    private $fileNameCacheCountries = 'stats-1-countries.cache';
    private $fileNameCacheBrowsers  = 'stats-1-browsers.cache';
    private $fileNameCacheRetention = 'stats-1-retention.cache';


    public function cacheLoad() {
        $this->countries = [];
        if (file_exists($this->dirCache . $this->fileNameCacheCountries)) {
            $this->countries = include $this->dirCache . $this->fileNameCacheCountries;
            $this->outputLog(self::OUTPUT_DEBUG, 'Reading cache: ' . $this->dirCache . $this->fileNameCacheCountries);
            if (!is_array($this->countries)) {
                outputLog(self::OUTPUT_ERRORS, '    🛑 error: not an array in: ' . $this->dirCache . $this->fileNameCacheCountries);
                $this->countries = [];
            }
        }

        $this->browsers = [];
        if (file_exists($this->dirCache . $this->fileNameCacheBrowsers)) {
            $this->browsers = include $this->dirCache . $this->fileNameCacheBrowsers;
            $this->outputLog(self::OUTPUT_DEBUG, 'Reading cache: ' . $this->dirCache . $this->fileNameCacheBrowsers);
            if (!is_array($this->browsers)) {
                outputLog(self::OUTPUT_ERRORS,     '🛑 error: not an array in: ' . $this->dirCache . $this->fileNameCacheBrowsers);
                $this->browsers = [];
            }
        }

        $this->retention = [];
        if (file_exists($this->dirCache . $this->fileNameCacheRetention)) {
            $this->retention = include $this->dirCache . $this->fileNameCacheRetention;
            $this->outputLog(self::OUTPUT_DEBUG, 'Reading cache: ' . $this->dirCache . $this->fileNameCacheRetention);
            if (!is_array($this->retention)) {
                outputLog(self::OUTPUT_ERRORS, '    🛑 error: not an array in: ' . $this->dirCache . $this->fileNameCacheRetention);
                $this->retention = [];
            }
        }

        $this->outputLog(self::OUTPUT_DEBUG, '');
        return true;
    }


    public function cacheSave(bool $foundNewCountries, bool $foundNewBrowsers, bool $foundNewRetention, string $lastDate) {
        if ($foundNewCountries) {
            $this->outputLog(self::OUTPUT_DEBUG, '    Writing cache: ' . $this->dirCache . $this->fileNameCacheCountries);
            $this->trimCountries();
            file_put_contents($this->dirCache . $this->fileNameCacheCountries, '<?php return ' . var_export($this->countries, true) . ';' . PHP_EOL);
        }

        if ($foundNewBrowsers) {
            $this->outputLog(self::OUTPUT_DEBUG, '    Writing cache: ' . $this->dirCache . $this->fileNameCacheBrowsers);
            $this->trimBrowsers();
            file_put_contents($this->dirCache . $this->fileNameCacheBrowsers, '<?php return ' . var_export($this->browsers, true) . ';' . PHP_EOL);
        }

        if ($foundNewRetention) {
            $this->outputLog(self::OUTPUT_DEBUG, '    Writing cache: ' . $this->dirCache . $this->fileNameCacheRetention);
            $this->trimRetention($lastDate);
            file_put_contents($this->dirCache . $this->fileNameCacheRetention, '<?php return ' . var_export($this->retention, true) . ';' . PHP_EOL);
        }
    }


    public function import(array $logFiles) {
        $gi = geoip_open($this->filePathGeoIpDB, GEOIP_STANDARD);
        foreach ($logFiles as $logFile) {
            $this->importLogFile($logFile, $gi);
        }
        geoip_close($gi);
    }


    public function importLogFile(string $logFile, $gi) {

        $stats = ['linesParsed' => 0];
        $dayVisitors = [];
        $linesTotal = 0;
        $linesRead = 0;
        $linesSkippedHead = 0;
        $linesSkippedTail = 0;
        $linesFromOtherDates = 0;
        $firstLine = true;
        $logDate = '';

        $foundNewCountries = false;
        $foundNewBrowsers = false;
        $foundNewRetention = false;


        $fhLog = fopen($logFile, 'r');
        if (!$fhLog) {
            $this->outputLog(self::OUTPUT_ERRORS, '🛑 error: could not open the log file: ' . escapeshellarg($logFile));
            return;
        }

        $this->outputLog(self::OUTPUT_INFO, 'Reading log: ' . $logFile);

        while (($line = fgets($fhLog)) !== false) {
            $linesTotal++;

            // parse log line with regex
            if (preg_match($this->logRegexPattern, $line, $m) !== 1) {
                $linesSkippedTail++;
                if ($firstLine) break;
                continue;
            }
            $m['cache'] = $m['cache'] ?? '';
            $m['speed'] = $m['speed'] ?? '';


            // parse log date and time with timezone
            $dt = \DateTime::createFromFormat('d/M/Y:H:i:s O', $m['datetime']);
            $date = $dt->format('Y-m-d');
            $hour = (int) $dt->format('H');
            $dateTime = $dt->format('Y-m-d H:i:s');


            // - get log day from first log line
            // - load saved stats for that day for linesParsed so we can skip already parsed lines
            //   this way hourly cron reduces total runtime
            // - prepare last dates for visitor and remove old retention records
            if ($firstLine) {
                $logDate = $date;
                $firstLine = false;

                $statsFileNameNoPrefix = $this->dirStats . $logDate . '.stats.php' . $this->compressStreamSuffix;
                $statsFileName = $this->compressStreamPrefix . $statsFileNameNoPrefix;
                if (file_exists($statsFileNameNoPrefix)) {
                    $this->outputLog(self::OUTPUT_DEBUG, '    reading stats file: ' . $statsFileName);
                    $stats = include $statsFileName;
                    if (!is_array($stats)) {
                        $this->outputLog(self::OUTPUT_ERRORS, '🛑 error: not an array in: ' . $statsFileName);
                        return;
                    }
                    if (isset($stats['date']) && $stats['date'] != $logDate) {
                        $this->outputLog(self::OUTPUT_ERRORS, '🛑 error: parsing log for: ' . $logDate . ' and existing stats are for: ' . $stats['date'] . ' on ' . $statsFileName);
                        return;
                    }
                } else {
                    $stats['date'] = $logDate;
                }

                $lastDates = [];
                $dtTemp = new \DateTime($date);
                for ($i = 0; $i < 7; $i++) {
                    $lastDate = $dtTemp->format('Y-m-d');
                    $lastDates[] = $lastDate;
                    $dtTemp->modify('- 1 day');
                }
                $this->trimRetention($lastDate);
            }



            // skip already parsed lines
            // if ($linesTotal <= $stats['linesParsed']) {
            //     $linesSkippedHead++;
            //     continue;
            // }




            // don't log other days than the first found in log
            if ($date != $logDate) {
                $linesFromOtherDates++;
                continue;
            }


            // get status type
            $sType = substr($m['status'], 0, 1) . 'xx';


            // get $uType and clean $url if needed
            $url = strtok($m['url'], '?');
            $uType = 'other';
            if (substr($url, 0, 7) == '/media/') {
                if (substr($url, 0, 14) == '/media/images/') {
                    $uType = 'images';
                    $url = substr(strrchr($url, '/'), 1);
                } else {
                    $uType = 'media';
                }
            } else if (substr($url, 0, 5) == '/gfx/') {
                $uType = 'gfx';
            } else if (substr($url, 0, 5) == '/css/') {
                $uType = 'css';
            } else if (substr($url, 0, 4) == '/js/') {
                $uType = 'js';
            } else if (substr($url, 0, 7) == '/fonts/') {
                $uType = 'font';
            } else if (strpos($url, '.') === false) {
                $uType = 'page';
            }



            // get country from ip and update last seen dt for sorting and trimming older ips.
            if (isset($this->countries[$m['ip']])) {
                $cc = $this->countries[$m['ip']]['cc'];
                $this->countries[$m['ip']]['dt'] = max($this->countries[$m['ip']]['dt'], $dateTime);
            } else {
                $cc = geoip_country_code_by_addr($gi, $m['ip']);
                $this->countries[$m['ip']] = [
                    'cc' => $cc,
                    'dt' => $dateTime,
                ];
                $foundNewCountries = true;
            }


            // parse user agent info with WhichBrowser (using cache)
            $uaHash = hash('fnv164', $m['ua']);
            if (isset($this->browsers[$uaHash])) {
                $ua = $this->browsers[$uaHash];
                $this->browsers[$uaHash]['dt'] = max($this->browsers[$uaHash]['dt'], $dateTime);
            } else {
                $foundNewBrowsers = true;
                $wb = new \WhichBrowser\Parser($m['ua']);
                if ($wb->device->type == 'bot') {
                    $ua = [
                        'type' => 'bot',
                        'brName' => $wb->browser->name ?? null,
                        'brVer' => $wb->browser->version->value ?? null,
                    ];
                } else {
                    $brVer = $wb->browser->version->value ?? '';
                    if ($brVer) {
                        $browserVerExploded = explode('.', $wb->browser->version->value);
                        if (count($browserVerExploded) > 2)
                            $brVer = $browserVerExploded[0] . '.' . $browserVerExploded[1];
                    }
                    $osVer = $wb->browser->version->value ?? '';
                    if ($osVer) {
                        $browserVerExploded = explode('.', $wb->browser->version->value);
                        if (count($browserVerExploded) > 2)
                            $osVer = $browserVerExploded[0] . '.' . $browserVerExploded[1];
                    }
                    $ua = [
                        'type' => $wb->device->type,
                        'brName' => $wb->browser->name ?? null,
                        'brVer' => $brVer,
                        'osName' => $wb->os->name ?? null,
                        'osVer' => $osVer,
                    ];
                }
                $this->browsers[$uaHash] = $ua;
                $this->browsers[$uaHash]['dt'] = $dateTime;
            }


            // parse speed into buckets
            $slow = false;
            $speed = '5.0+';
            $speedFloat = floatval($m['speed']);
            if ($speedFloat == 0) {
                $speed = '0.00';
            } else if ($speedFloat < 0.02) {
                $speed = '0.02';
            } else if ($speedFloat < 0.04) {
                $speed = '0.04';
            } else if ($speedFloat < 0.06) {
                $speed = '0.06';
            } else if ($speedFloat < 0.08) {
                $speed = '0.08';
            } else if ($speedFloat < 0.1) {
                $speed = '0.1';
            } else if ($speedFloat < 0.2) {
                $speed = '0.2';
            } else if ($speedFloat < 0.4) {
                $speed = '0.4';
            } else if ($speedFloat < 0.6) {
                $speed = '0.6';
            } else if ($speedFloat < 0.8) {
                $speed = '0.8';
            } else if ($speedFloat < 1.0) {
                $speed = '1.0';
            } else if ($speedFloat < 5.0) {
                $speed = floor($speedFloat) . '.0';
                $slow = true;
            }


            // new visitors and visitor retention
            $visitorHash = hash('fnv164', $m['ip'] . $m['ua']);
            $dayVisitorNew = false;
            $dayVisitorNewUrl = false;

            if (!isset($dayVisitors[$visitorHash])) {
                $dayVisitorNew = true;
                $dayVisitors[$visitorHash] = [];
            }

            if ($uType == 'page' && $sType == '2xx' && !isset($this->retention[$date][$visitorHash]['urls'][$url])) {
                $foundNewRetention = true;
                $dayVisitorNewUrl = true;
                $this->retention[$date][$visitorHash]['pages'] = ($this->retention[$date][$visitorHash]['pages'] ?? 0) + 1;
                $this->retention[$date][$visitorHash]['urls'][$url] = true;
            }




            // construct log for day

            $vst = 'ppl';
            if ($ua['type'] == 'bot') $vst = 'bot';

            // totals
            if ($dayVisitorNew) $stats['total']['visitor'] = ($stats['total']['visitor'] ?? 0) + 1;
            $stats['total']['hit'] = ($stats['total']['hit'] ?? 0) + 1;
            $stats['total']['uType'][$uType] = ($stats['total']['uType'][$uType] ?? 0) + 1;
            $stats['total']['status'][$sType] = ($stats['total']['status'][$sType] ?? 0) + 1;
            $stats['total']['size']['total'] = ($stats['total']['size']['total'] ?? 0) + $m['bytes'];
            $stats['total']['size'][$uType] = ($stats['total']['size'][$uType] ?? 0) + $m['bytes'];
            $stats['total']['cache'][$m['cache']] = ($stats['total']['cache'][$m['cache']] ?? 0) + 1;
            $stats['total']['speed'][$speed] = ($stats['total']['speed'][$speed] ?? 0) + 1;
            if ($slow) $stats['total']['slow'][$uType][$url] = ($stats['total']['slow'][$uType][$url] ?? 0) + 1;

            // totals for robots and people
            if ($dayVisitorNew) $stats[$vst]['total']['visitor'] = ($stats[$vst]['total']['visitor'] ?? 0) + 1;
            $stats[$vst]['total']['hit'] = ($stats[$vst]['total']['hit'] ?? 0) + 1;
            $stats[$vst]['total']['uType'][$uType] = ($stats[$vst]['total']['uType'][$uType] ?? 0) + 1;
            $stats[$vst]['total']['status'][$sType] = ($stats[$vst]['total']['status'][$sType] ?? 0) + 1;
            $stats[$vst]['total']['size']['total'] = ($stats[$vst]['total']['size']['total'] ?? 0) + $m['bytes'];
            $stats[$vst]['total']['size'][$uType] = ($stats[$vst]['total']['size'][$uType] ?? 0) + $m['bytes'];
            $stats[$vst]['total']['cache'][$m['cache']] = ($stats[$vst]['total']['cache'][$m['cache']] ?? 0) + 1;
            $stats[$vst]['total']['speed'][$speed] = ($stats[$vst]['total']['speed'][$speed] ?? 0) + 1;


            if ($vst == 'bot') {

                // all robots
                if ($dayVisitorNew) $stats[$vst]['name']['total'][$sType][$uType]['visitor'] = ($stats[$vst]['name']['total'][$sType][$uType]['visitor'] ?? 0) + 1;
                $stats[$vst]['name']['total'][$sType][$uType]['hit']                         = ($stats[$vst]['name']['total'][$sType][$uType]['hit'] ?? 0) + 1;
                $stats[$vst]['name']['total'][$sType][$uType]['hour'][$hour]                 = ($stats[$vst]['name']['total'][$sType][$uType]['hour'][$hour] ?? 0) + 1;

                // all robots per page
                if ($dayVisitorNewUrl) $stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['visitor'] = ($stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['visitor'] ?? 0) + 1;
                $stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['hit']                            = ($stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['hit'] ?? 0) + 1;
                $stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['size']                           = ($stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['size'] ?? 0) + $m['bytes'];
                $stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['cache'][$m['cache']]             = ($stats[$vst]['name']['total'][$sType][$uType]['url'][$url]['cache'][$m['cache']] ?? 0) + 1;

                // by robot
                if ($dayVisitorNew) $stats[$vst]['name'][$ua['brName']][$sType][$uType]['visitor'] = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['visitor'] ?? 0) + 1;
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['hit']                         = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['hit'] ?? 0) + 1;
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['hour'][$hour]                 = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['hour'][$hour] ?? 0) + 1;
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['version'][$ua['brVer']]       = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['version'][$ua['brVer']] ?? 0) + 1;

                // by robot per page
                if ($dayVisitorNewUrl) $stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['visitor'] = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['visitor'] ?? 0) + 1;
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['hit']                            = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['hit'] ?? 0) + 1;
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['size']                           = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['size'] ?? 0) + $m['bytes'];
                $stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['cache'][$m['cache']]             = ($stats[$vst]['name'][$ua['brName']][$sType][$uType]['url'][$url]['cache'][$m['cache']] ?? 0) + 1;

            } else {

                // all countries
                if ($dayVisitorNew) $stats[$vst]['cc']['total'][$sType][$uType]['visitor']                 = ($stats[$vst]['cc']['total'][$sType][$uType]['visitor'] ?? 0) + 1;
                $stats[$vst]['cc']['total'][$sType][$uType]['hit']                                         = ($stats[$vst]['cc']['total'][$sType][$uType]['hit'] ?? 0) + 1;
                $stats[$vst]['cc']['total'][$sType][$uType]['hour'][$hour]                                 = ($stats[$vst]['cc']['total'][$sType][$uType]['hour'][$hour] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua']['total']['type'][$ua['type']]                                   = ($stats[$vst]['cc']['total']['ua']['total']['type'][$ua['type']] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua']['total']['os'][$ua['osName']]                                   = ($stats[$vst]['cc']['total']['ua']['total']['os'][$ua['osName']] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua'][$ua['type']]['browser'][$ua['brName']]['total']                 = ($stats[$vst]['cc']['total']['ua'][$ua['type']]['browser'][$ua['brName']]['total'] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua'][$ua['type']]['browser'][$ua['brName']]['version'][$ua['brVer']] = ($stats[$vst]['cc']['total']['ua'][$ua['type']]['browser'][$ua['brName']]['version'][$ua['brVer']] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua'][$ua['type']]['os'][$ua['osName']]['total']                      = ($stats[$vst]['cc']['total']['ua'][$ua['type']]['os'][$ua['osName']]['total'] ?? 0) + 1;
                $stats[$vst]['cc']['total']['ua'][$ua['type']]['os'][$ua['osName']]['version'][$ua['osVer']]      = ($stats[$vst]['cc']['total']['ua'][$ua['type']]['os'][$ua['osName']]['version'][$ua['osVer']] ?? 0) + 1;

                // all countries per page
                if ($dayVisitorNewUrl) $stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['visitor'] = ($stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['visitor'] ?? 0) + 1;
                $stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['hit']                            = ($stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['hit'] ?? 0) + 1;
                $stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['size']                           = ($stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['size'] ?? 0) + $m['bytes'];
                $stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['cache'][$m['cache']]             = ($stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['cache'][$m['cache']] ?? 0) + 1;

                // by country
                if ($dayVisitorNew) $stats[$vst]['cc'][$cc][$sType][$uType]['visitor']                 = ($stats[$vst]['cc'][$cc][$sType][$uType]['visitor'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc][$sType][$uType]['hit']                                         = ($stats[$vst]['cc'][$cc][$sType][$uType]['hit'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc][$sType][$uType]['hour'][$hour]                                 = ($stats[$vst]['cc'][$cc][$sType][$uType]['hour'][$hour] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua']['total']['type'][$ua['type']]                                   = ($stats[$vst]['cc'][$cc]['ua']['total']['type'][$ua['type']] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua']['total']['os'][$ua['osName']]                                   = ($stats[$vst]['cc'][$cc]['ua']['total']['os'][$ua['osName']] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua'][$ua['type']]['browser'][$ua['brName']]['total']                 = ($stats[$vst]['cc'][$cc]['ua'][$ua['type']]['browser'][$ua['brName']]['total'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua'][$ua['type']]['browser'][$ua['brName']]['version'][$ua['brVer']] = ($stats[$vst]['cc'][$cc]['ua'][$ua['type']]['browser'][$ua['brName']]['version'][$ua['brVer']] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua'][$ua['type']]['os'][$ua['osName']]['total']                      = ($stats[$vst]['cc'][$cc]['ua'][$ua['type']]['os'][$ua['osName']]['total'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc]['ua'][$ua['type']]['os'][$ua['osName']]['version'][$ua['osVer']]      = ($stats[$vst]['cc'][$cc]['ua'][$ua['type']]['os'][$ua['osName']]['version'][$ua['osVer']] ?? 0) + 1;

                // by country per page
                if ($dayVisitorNewUrl) $stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['visitor'] = ($stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['visitor'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['hit']                            = ($stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['hit'] ?? 0) + 1;
                $stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['size']                           = ($stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['size'] ?? 0) + $m['bytes'];
                $stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['cache'][$m['cache']]             = ($stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['cache'][$m['cache']] ?? 0) + 1;

                // retention for all countries and each country
                foreach ($this->retention as $dateTemp => $this->retentionVisitors) {
                    if (isset($this->retention[$dateTemp][$visitorHash]['urls'][$url])) {
                        $stats[$vst]['cc']['total'][$sType][$uType]['retention'][$dateTemp]              = ($stats[$vst]['cc']['total'][$sType][$uType]['retention'][$dateTemp] ?? 0) + 1;
                        $stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['retention'][$dateTemp] = ($stats[$vst]['cc']['total'][$sType][$uType]['url'][$url]['retention'][$dateTemp] ?? 0) + 1;
                        $stats[$vst]['cc'][$cc][$sType][$uType]['retention'][$dateTemp]                  = ($stats[$vst]['cc'][$cc][$sType][$uType]['retention'][$dateTemp] ?? 0) + 1;
                        $stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['retention'][$dateTemp]     = ($stats[$vst]['cc'][$cc][$sType][$uType]['url'][$url]['retention'][$dateTemp] ?? 0) + 1;
                    }
                }

            }


            // trim cache arrays every X lines to lower memory usage
            if ($linesRead % 1000 == 0) {
                if ($foundNewCountries) $this->trimCountries();
                if ($foundNewBrowsers)  $this->trimBrowsers();
                if ($foundNewRetention) $this->trimRetention($lastDate);
            }


            $linesRead++;
        }
        fclose($fhLog);


        $this->cacheSave($foundNewCountries, $foundNewBrowsers, $foundNewRetention, $lastDate);


        $this->outputLog(self::OUTPUT_COMPLETE, '    lines read: ' . $linesRead . '/' . $linesTotal . '. Skipped head: ' . $linesSkippedHead . '. Skipped Tail: ' . $linesSkippedTail . '. Other dates: ' . $linesFromOtherDates);
        $stats['linesParsed'] = $linesTotal;
        if ($linesRead) {
            $this->outputLog(self::OUTPUT_INFO, '    saving stats: ' . $statsFileName);
            file_put_contents($statsFileName, '<?php return ' . var_export($stats, true) . ';' . PHP_EOL);
        }
        $this->outputLog(self::OUTPUT_INFO, '');
    }




    private function trimCountries() {
        uasort($this->countries, function($a, $b) {
            return ($b['dt'] ?? '0') <=> ($a['dt'] ?? '0');
        });
        $this->countries = array_slice($this->countries, 0, 5000);
    }


    private function trimBrowsers() {
        uasort($this->browsers, function($a, $b) {
            return ($b['dt'] ?? '0') <=> ($a['dt'] ?? '0');
        });
        $this->browsers = array_slice($this->browsers, 0, 500);
    }


    private function trimRetention(string $lastDate) {
        foreach ($this->retention as $dateTemp => $this->retentionVisitors) {
            if ($dateTemp < $lastDate) {
                unset($this->retention[$dateTemp]);
                continue;
            }
            uasort($this->retention[$dateTemp], function($a, $b) {
                return ($b['pages'] ?? 0) <=> ($a['pages'] ?? 0);
            });
            $this->retention[$dateTemp] = array_slice($this->retention[$dateTemp], 0, 5000);
        }
    }

    private function outputLog(int $level, string $msg) {
        if ($this->cli) {
            if ($this->verbosity >= $level) echo $msg . PHP_EOL;
        } else {
            if ($msg != '') $this->output[] = $msg;
        }
    }

}
