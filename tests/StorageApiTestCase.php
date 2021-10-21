<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

namespace Keboola\Test;

use Keboola\StorageApi\BranchAwareGuzzleClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Tokens;
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
    const BACKEND_EXASOL = 'exasol';

    const STAGE_IN = 'in';
    const STAGE_OUT = 'out';
    const STAGE_SYS = 'sys';

    const ISO8601_REGEXP = '/^([0-9]{4})-(1[0-2]|0[1-9])-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\+([0-9]{4})$/';

    const FILE_LONG_TERM_EXPIRATION_IN_DAYS = 15;
    const FILE_SHORT_TERM_EXPIRATION_IN_DAYS = 2;

    protected $_bucketIds = array();

    /** @var Client */
    protected $_client;

    /** @var Tokens */
    protected $tokens;

    /**
     * @var string
     */
    protected $tokenId;

    /**
     * @var string
     */
    protected $lastEventId;

    /**
     * @param $testName
     * @return string
     */
    public function getTestBucketName($testName)
    {
        return sprintf('API-tests-' . sha1($testName));
    }

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
        $this->tokens = new Tokens($this->_client);
    }

    protected function _initEmptyTestBuckets($stages = [self::STAGE_OUT, self::STAGE_IN])
    {
        foreach ($stages as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-tests', $stage, 'API-tests');
        }
    }

    protected function initEmptyTestBucketsForParallelTests($stages = [self::STAGE_OUT, self::STAGE_IN])
    {
        $description = $this->generateDescriptionForTestObject();
        foreach ($stages as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket($this->getTestBucketName($description), $stage, $description);
        }
    }

    protected function listTestBucketsForParallelTests($stages = [self::STAGE_OUT, self::STAGE_IN])
    {
        $description = $this->generateDescriptionForTestObject();
        $bucketName = sprintf('API-tests-' . sha1($description));
        $buckets = [];
        foreach ($stages as $stage) {
            $bucketId = $stage . '.c-' . $bucketName;
            $buckets[] = $this->_client->getBucket($bucketId);
        }

        return $buckets;
    }

    /**
     * Init empty bucket test helper
     * @param $name
     * @param $stage
     * @return bool|string
     */
    protected function initEmptyBucket($name, $stage, $description, Client $client = null)
    {
        if (!$client) {
            $client = $this->_client;
        }

        try {
            $bucket = $client->getBucket("$stage.c-$name");
            // unlink and unshare buckets if they exist
            if ($client->isSharedBucket($bucket['id'])) {
                if (array_key_exists('linkedBy', $bucket)) {
                    foreach ($bucket['linkedBy'] as $linkedBucket) {
                        try {
                            $client->forceUnlinkBucket(
                                $bucket['id'],
                                $linkedBucket['project']['id']
                            );
                        } catch (\Keboola\StorageApi\ClientException $e) {
                            $this->throwExceptionIfNotDeleted($e);
                        }
                    }
                }
                $client->unshareBucket($bucket['id']);
            }
            $tables = $client->listTables($bucket['id']);
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
                    $client->dropTable($table['id']);
                } catch (\Keboola\StorageApi\ClientException $e) {
                    $this->throwExceptionIfNotDeleted($e);
                }
            }
            $metadataApi = new Metadata($client);
            $metadata = $metadataApi->listBucketMetadata($bucket['id']);
            foreach ($metadata as $md) {
                try {
                    $metadataApi->deleteBucketMetadata($bucket['id'], $md['id']);
                } catch (\Keboola\StorageApi\ClientException $e) {
                    $this->throwExceptionIfNotDeleted($e);
                }
            }
            return $bucket['id'];
        } catch (\Keboola\StorageApi\ClientException $e) {
            if ($e->getCode() === 500) {
                throw $e;
            }
            if ($e->getCode() === 403) {
                throw $e;
            }
            return $client->createBucket($name, $stage, $description);
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
    private function throwExceptionIfNotDeleted(\Keboola\StorageApi\ClientException $e)
    {
        // this could take backoff and second run will be 404
        // we don't care is table dont exists
        if ($e->getCode() !== 404) {
            throw $e;
        }
    }

    protected function getExportFilePathForTest($fileName)
    {
        $testName = $this->generateDescriptionForTestObject();
        return __DIR__ . '/_tmp/' . sha1($testName) . '.' . $fileName;
    }

    /**
     * @param string $branchPrefix
     */
    protected function deleteBranchesByPrefix(DevBranches $devBranches, $branchPrefix)
    {
        $branchesList = $devBranches->listBranches();
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchPrefix) {
                return strpos($branch['name'], $branchPrefix) === 0;
            }
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            $devBranches->deleteBranch($branch['id']);
        }
    }

    protected function generateBranchNameForParallelTest($suffix = null)
    {
        $providedToken = $this->_client->verifyToken();

        $name = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];

        if ($suffix && $suffix !== '') {
            $name .= '\\' . $suffix;
        }

        return $name;
    }

    /**
     * @return string
     */
    protected function generateDescriptionForTestObject()
    {
        $testSuiteName = '';
        if (SUITE_NAME) {
            $testSuiteName = sprintf('%s::', SUITE_NAME);
        }

        return $testSuiteName . get_class($this) . '\\' . $this->getName();
    }

    /**
     * @return int
     */
    protected function getRedshiftNodeCount()
    {
        if (REDSHIFT_NODE_COUNT) {
            return (int) REDSHIFT_NODE_COUNT;
        }

        return 1;
    }

    protected function listJobsByRunId($runId)
    {
        return array_filter(
            $this->_client->listJobs(),
            function ($job) use ($runId) {
                return $job['runId'] === $runId;
            }
        );
    }

    /**
     * Usage:
     *  - `$getClient($this)` -> return default client
     *  - `$getClient($this, [...])` -> return client with custom config
     *  - `$getClient($this, [], true)` -> return default client which use same branch as before
     *  - `$getClient($this, [...], true)` -> return client with custom which use same branch as before
     *
     * DataProvider cannot use `$this->variable` because the provider run before the `setUp` method is called.
     * Need to put `$this` as argument to anonymous function to build correct variable values and return correct client.
     *
     * > All data providers are executed before both the call to the setUpBeforeClass static method
     * > and the first call to the setUp method.
     * > Because of that you can't access any variables you create there from within a data provider.
     * > This is required in order for PHPUnit to be able to compute the total number of tests.
     * > https://phpunit.de/manual/6.5/en/writing-tests-for-phpunit.html#:~:text=a%20depending%20test.-,Note,-All%20data%20providers
     */
    public function provideComponentsClient()
    {
        yield 'defaultBranch' => [
            function (self $that, array $config = []) {
                if ($config) {
                    return $that->getClient($config);
                } else {
                    return $that->_client;
                }
            }
        ];

        yield 'devBranch' => [
            function (self $that, array $config = [], $useExistingBranch = false) {
                $branch = $this->createOrReuseDevBranch($that, $useExistingBranch);

                if ($config) {
                    return $this->getBranchAwareClient($branch['id'], $config);
                } else {
                    return $this->getBranchAwareDefaultClient($branch['id']);
                }
            }
        ];
    }

    /**
     * Usage:
     *  - `$getGuzzleClient($this, [...])` -> return Guzzle client with custom config
     *  - `$getGuzzleClient($this, [...], true)` -> return Guzzle client with custom which use same branch as before
     *
     * @see provideComponentsClient
     */
    public function provideComponentsGuzzleClient()
    {
        yield 'defaultBranch' => [
            function (self $that, array $config) {
                return new \GuzzleHttp\Client($config);
            }
        ];

        yield 'devBranch' => [
            function (self $that, array $config, $useExistingBranch = false) {
                $branch = $this->createOrReuseDevBranch($that, $useExistingBranch);
                return new BranchAwareGuzzleClient($branch['id'], $config);
            }
        ];
    }

    /**
     * Usage:
     *  - `$getClient($this)` -> return default client which use branch aware client for calling default branch
     *  - `$getClient($this, [...])` -> return client with custom config which use branch aware client for calling default branch
     *  - `$getClient($this, [], true)` -> return default client which use same branch as before
     *  - `$getClient($this, [...], true)` -> return client with custom which use same branch as before
     *
     * @see provideComponentsClient
     */
    public function provideBranchAwareComponentsClient()
    {
        yield 'defaultBranch' => [
            function (self $that, array $config = []) {
                if ($config) {
                    return $this->getBranchAwareClient($this->getDefaultBranchId($that), $config);
                } else {
                    return $this->getBranchAwareDefaultClient($this->getDefaultBranchId($that));
                }
            }
        ];

        yield 'devBranch' => [
            function (self $that, array $config = [], $useExistingBranch = false) {
                $branch = $this->createOrReuseDevBranch($that, $useExistingBranch);

                if ($config) {
                    return $this->getBranchAwareClient($branch['id'], $config);
                } else {
                    return $this->getBranchAwareDefaultClient($branch['id']);
                }
            }
        ];
    }

    protected function getDefaultBranchId(self $that)
    {
        $devBranch = new \Keboola\StorageApi\DevBranches($that->_client);
        $branchesList = $devBranch->listBranches();
        foreach ($branchesList as $branch) {
            if ($branch['isDefault'] === true) {
                return $branch['id'];
            }
        }

        throw new \Exception('Default branch not found.');
    }

    public function cleanupConfigurations()
    {
        $components = new Components($this->_client);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }
    }

    /**
     * @param string $eventName
     * @return array
     */
    protected function listEvents(Client $client, $eventName, $expectedObjectId = null)
    {
        return $this->retry(function () use ($client, $expectedObjectId) {
            $tokenEvents = $client->listEvents([
                'sinceId' => $this->lastEventId,
                'limit' => 1,
                'q' => sprintf('token.id:%s', $this->tokenId),
            ]);

            if ($expectedObjectId === null) {
                return $tokenEvents;
            }

            return array_filter($tokenEvents, function ($event) use ($expectedObjectId) {
                return $event['objectId'] === $expectedObjectId;
            });
        }, 10, $eventName);
    }

    /**
     * @param callable $apiCall
     * @param int $retries
     * @param string $eventName
     * @return array
     */
    protected function retry($apiCall, $retries, $eventName)
    {
        $events = [];
        while ($retries > 0) {
            $events = $apiCall();
            if (empty($events) || $events[0]['event'] !== $eventName) {
                $retries--;
                usleep(250 * 1000);
            } else {
                break;
            }
        }
        return $events;
    }

    protected function assertEvent(
        $event,
        $expectedEventName,
        $expectedEventMessage,
        $expectedObjectId,
        $expectedObjectName,
        $expectedObjectType,
        $expectedParams
    ) {
        self::assertArrayHasKey('objectName', $event);
        self::assertEquals($expectedObjectName, $event['objectName']);
        self::assertArrayHasKey('objectType', $event);
        self::assertEquals($expectedObjectType, $event['objectType']);
        self::assertArrayHasKey('objectId', $event);
        self::assertEquals($expectedObjectId, $event['objectId']);
        self::assertArrayHasKey('event', $event);
        self::assertEquals($expectedEventName, $event['event']);
        self::assertArrayHasKey('message', $event);
        self::assertEquals($expectedEventMessage, $event['message']);
        self::assertArrayHasKey('token', $event);
        self::assertEquals($this->tokenId, $event['token']['id']);
        self::assertArrayHasKey('params', $event);
        self::assertSame($expectedParams, $event['params']);
    }

    protected function createOrReuseDevBranch(self $that, $useExistingBranch = false)
    {
        $providedToken = $that->_client->verifyToken();
        $branchName = implode('\\', [
            __CLASS__,
            $that->getName(false),
            $that->dataName(),
            $providedToken['id'],
        ]);
        $devBranch = new \Keboola\StorageApi\DevBranches($that->_client);

        if ($useExistingBranch) {
            $branches = $devBranch->listBranches();
            $branch = null;
            // get branch detail
            foreach ($branches as $branchItem) {
                if ($branchItem['name'] === $branchName) {
                    $branch = $branchItem;
                }
            }
            if (!isset($branch)) {
                $this->fail(sprintf('Reuse existing branch: branch %s not found.', $branchName));
            }
        } else {
            // create new branch
            $this->deleteBranchesByPrefix($devBranch, $branchName);
            $branch = $devBranch->createBranch($branchName);
        }

        return $branch;
    }

    /**
     * @param string $name
     * @return string
     */
    public function generateUniqueNameForString($name)
    {
        return sha1($this->generateDescriptionForTestObject()) . '_' . $name;
    }

    protected function initEvents()
    {
        $this->tokenId = $this->_client->verifyToken()['id'];
        $lastEvent = $this->_client->listTokenEvents($this->tokenId, [
            'limit' => 1,
        ]);
        if (!empty($lastEvent)) {
            $this->lastEventId = $lastEvent[0]['id'];
        }
    }
}
