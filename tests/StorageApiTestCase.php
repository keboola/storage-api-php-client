<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

namespace Keboola\Test;

use function array_key_exists;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;

abstract class StorageApiTestCase extends ClientTestCase
{
    const BACKEND_REDSHIFT = 'redshift';
    const BACKEND_SNOWFLAKE = 'snowflake';
    const BACKEND_SYNAPSE = 'synapse';

    const STAGE_IN = 'in';
    const STAGE_OUT = 'out';
    const STAGE_SYS = 'sys';

    const ISO8601_REGEXP = '/^([0-9]{4})-(1[0-2]|0[1-9])-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\+([0-9]{4})$/';

    const FILE_LONG_TERM_EXPIRATION_IN_DAYS = 15;
    const FILE_SHORT_TERM_EXPIRATION_IN_DAYS = 2;

    protected $_bucketIds = array();

    /**
     * @var \Keboola\StorageApi\Client
     */
    protected $_client;

    /**
     * Checks that two arrays are same except some keys, which MUST be different
     *
     * @param array $expected
     * @param array $actual
     * @param array $exceptKeys
     */
    public function assertArrayEqualsExceptKeys($expected, $actual, array $exceptKeys)
    {
        foreach ($exceptKeys as $exceptKey) {
            static::assertArrayHasKey($exceptKey, $actual);
            if (array_key_exists($exceptKey, $expected) && array_key_exists($exceptKey, $actual)) {
                static::assertNotEquals($expected[$exceptKey], $actual[$exceptKey]);
            }
            unset($actual[$exceptKey]);
            unset($expected[$exceptKey]);
        }
        static::assertEquals($expected, $actual);
    }

    /**
     * Asserts that two arrays are equal, ignoring some keys. That keys may or
     * may not be present.
     *
     * @param array $expected
     * @param array $actual
     * @param array $ignoreKeys
     */
    public function assertArrayEqualsIgnoreKeys($expected, $actual, array $ignoreKeys)
    {
        foreach ($ignoreKeys as $ignoreKey) {
            unset($actual[$ignoreKey]);
            unset($expected[$ignoreKey]);
        }
        static::assertEquals($expected, $actual);
    }

    public function setUp()
    {
        $this->_client = $this->getDefaultClient();
    }

    protected function _initEmptyTestBuckets($stages = [self::STAGE_OUT, self::STAGE_IN])
    {
        foreach ($stages as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-tests', $stage, 'API-tests');
        }
    }

    protected function initEmptyTestBucketsForParallelTests($stages = [self::STAGE_OUT, self::STAGE_IN])
    {
        $description = get_class($this) . '\\' . $this->getName();
        $bucketName = sprintf('API-tests-' . sha1($description));
        foreach ($stages as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket($bucketName, $stage, $description);
        }
    }

    /**
     * Init empty bucket test helper
     * @param $name
     * @param $stage
     * @return bool|string
     */
    private function initEmptyBucket($name, $stage, $description)
    {
        try {
            $bucket = $this->_client->getBucket("$stage.c-$name");
            // unlink and unshare buckets if they exist
            if ($this->_client->isSharedBucket($bucket['id'])) {
                if (array_key_exists('linkedBy', $bucket)) {
                    foreach ($bucket['linkedBy'] as $linkedBucket) {
                        try {
                            $this->_client->dropBucket($linkedBucket['id'], ['force' => true]);
                        } catch (\Keboola\StorageApi\ClientException $e) {
                            $this->throwExceptionIfDeleted($e);
                        }
                    }
                }
                $this->_client->unshareBucket($bucket['id']);
            }
            $tables = $this->_client->listTables($bucket['id']);
            // iterate in reverse creation order to be able to delete aliases
            usort($tables, function ($table1, $table2) {
                $timestamp1 = strtotime($table1['created']);
                $timestamp2 = strtotime($table2['created']);
                if ($timestamp1 == $timestamp2) {
                    return 0;
                }
                return ($timestamp1 < $timestamp2) ? -1 : 1;
            });
            foreach (array_reverse($tables) as $table) {
                try {
                    $this->_client->dropTable($table['id']);
                } catch (\Keboola\StorageApi\ClientException $e) {
                    $this->throwExceptionIfDeleted($e);
                }
            }
            $metadataApi = new Metadata($this->_client);
            $metadata = $metadataApi->listBucketMetadata($bucket['id']);
            foreach ($metadata as $md) {
                try {
                    $metadataApi->deleteBucketMetadata($bucket['id'], $md['id']);
                } catch (\Keboola\StorageApi\ClientException $e) {
                    $this->throwExceptionIfDeleted($e);
                }
            }
            return $bucket['id'];
        } catch (\Keboola\StorageApi\ClientException $e) {
            if ($e->getCode() === 500) {
                throw $e;
            }
            return $this->_client->createBucket($name, $stage, $description);
        }
    }

    /**
     * @param $path
     * @return array
     */
    protected function _readCsv($path, $delimiter = ",", $enclosure = '"', $escape = '"')
    {
        $fh = fopen($path, 'r');
        $lines = array();
        while (($data = fgetcsv($fh, 1000, $delimiter, $enclosure, $escape)) !== false) {
            $lines[] = $data;
        }
        fclose($fh);
        return $lines;
    }

    public function assertLinesEqualsSorted($expected, $actual, $message = "")
    {
        $expected = explode("\n", $expected);
        $actual = explode("\n", $actual);

        sort($expected);
        sort($actual);
        $this->assertEquals($expected, $actual, $message);
    }

    public function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
    {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        $this->assertEquals($expected, $actual, $message);
    }

    public function tableExportFiltersData()
    {
        return array(
            // first test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'columns' => ['id', 'name', 'sex'],
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "female",
                    ),
                ),
            ),
            // first test with defined operator
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'whereOperator' => 'eq',
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                ),
            ),
            // second test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN')
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // third test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // fourth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN'),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // fifth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
                    'whereOperator' => 'eq'
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // sixth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
        );
    }

    protected function getTestBucketId($stage = self::STAGE_IN)
    {
        return $this->_bucketIds[$stage];
    }

    protected function createAndWaitForEvent(Event $event, Client $sapiClient = null)
    {
        $client = null !== $sapiClient ? $sapiClient : $this->_client;

        $id = $client->createEvent($event);

        sleep(2); // wait for ES refresh
        $tries = 0;
        while (true) {
            try {
                return $client->getEvent($id);
            } catch (\Keboola\StorageApi\ClientException $e) {
                echo 'Event not found: ' . $id . PHP_EOL;
            }
            if ($tries > 4) {
                throw new \Exception('Max tries exceeded.');
            }
            $tries++;
            sleep(pow(2, $tries));
        }
    }

    protected function createAndWaitForFile($path, FileUploadOptions $options, Client $sapiClient = null)
    {
        $client = $sapiClient ? $sapiClient : $this->_client;

        $fileId = $client->uploadFile($path, $options);
        return $this->waitForFile($fileId, $client);
    }

    protected function waitForFile($fileId, $sapiClient = null)
    {
        $client = $sapiClient ? $sapiClient : $this->_client;
        $fileSearchOptions = new ListFilesOptions();
        $fileSearchOptions = $fileSearchOptions->setQuery(sprintf("id:%s", $fileId));

        $tries = 0;
        sleep(2);
        while (true) {
            try {
                $files = $client->listFiles($fileSearchOptions);
                if (count($files) && $files[0]['id'] === $fileId) {
                    return $fileId;
                }
            } catch (\Keboola\StorageApi\ClientException $e) {
                echo 'File not found: ' . $fileId . PHP_EOL;
            }
            if ($tries > 4) {
                throw new \Exception('Max tries exceeded.');
            }
            $tries++;
            sleep(pow(2, $tries));
        }
    }

    protected function createTableWithRandomData($tableName, $rows = 5, $columns = 10, $charsInCell = 20)
    {
        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $header = [];
        for ($i = 0; $i++ < $columns;) {
            $header[] = 'col_' . $i;
        }
        $csvFile->writeRow($header);
        for ($i = 0; $i++ < $rows;) {
            $row = [];
            for ($j = 0; $j++ < $columns;) {
                $row[] = $this->createRandomString($charsInCell);
            }
            $csvFile->writeRow($row);
        }
        return $this->_client->createTable($this->getTestBucketId(), $tableName, $csvFile);
    }

    /**
     * @param int $length
     * @return string
     */
    private function createRandomString($length)
    {
        $alpabet = "abcdefghijklmnopqrstvuwxyz0123456789 ";
        $randStr = "";
        for ($i = 0; $i < $length; $i++) {
            $randStr .=  $alpabet[rand(0, strlen($alpabet)-1)];
        }
        return $randStr;
    }

    /**
     * @param Client $client
     * @param string $testBucketId
     */
    public function dropBucketIfExists($client, $testBucketId, $async = false)
    {
        if ($client->bucketExists($testBucketId)) {
            $client->dropBucket($testBucketId, ['force' => true, 'async' => $async]);
        }
    }

    protected function findLastEvent(Client $client, array $filter)
    {
        $this->createAndWaitForEvent(
            (new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'),
            $client
        );
        $events = $client->listTokenEvents($client->verifyToken()['id']);
        foreach ($events as $event) {
            foreach ($filter as $key => $value) {
                if ($event[$key] != $value) {
                    continue 2;
                }
            }
            return $event;
        }
        $this->fail(sprintf('Event for filter "%s" does not exist', (string) json_encode($filter)));
    }

    /**
     * @throws \Keboola\StorageApi\ClientException
     */
    private function throwExceptionIfDeleted(\Keboola\StorageApi\ClientException $e)
    {
        // this could take backoff and second run will be 404
        // we don't care is table dont exists
        if ($e->getCode() !== 404) {
            throw $e;
        }
    }
}
