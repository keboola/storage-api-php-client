<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

namespace Keboola\Test;

use Exception;
use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Tokens;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\Utils\EventTesterUtils;
use function array_key_exists;

abstract class StorageApiTestCase extends ClientTestCase
{
    use EventTesterUtils;

    const BACKEND_REDSHIFT = 'redshift';
    const BACKEND_SNOWFLAKE = 'snowflake';
    const BACKEND_SYNAPSE = 'synapse';
    const BACKEND_EXASOL = 'exasol';
    const BACKEND_TERADATA = 'teradata';
    const BACKEND_BIGQUERY = 'bigquery';

    public const CUSTOM_QUERY_MANAGER_COMPONENT_ID = 'keboola.app-custom-query-manager';

    const STAGE_IN = 'in';
    const STAGE_OUT = 'out';
    const STAGE_SYS = 'sys';

    const ISO8601_REGEXP = '/^([0-9]{4})-(1[0-2]|0[1-9])-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\+([0-9]{4})$/';

    const FILE_LONG_TERM_EXPIRATION_IN_DAYS = 15;
    const FILE_SHORT_TERM_EXPIRATION_IN_DAYS = 2;

    const FEATURE_CONFIGURATIONS_USE_DEV_BRANCH_SERVICES_ONLY = 'configurations-use-dev-branch-services-only';

    protected $_bucketIds = [];

    /** @var Client */
    protected $_client;

    /** @var Tokens */
    protected $tokens;

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

    public function setUp(): void
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
     * @return string
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
    protected function _readCsv($path, $delimiter = ',', $enclosure = '"', $escape = '"')
    {
        $fh = fopen($path, 'r');
        if ($fh === false) {
            throw new Exception(sprintf('Cannot open file "%s"', $path));
        }
        $lines = [];
        while (($data = fgetcsv($fh, 1000, $delimiter, $enclosure, $escape)) !== false) {
            $lines[] = $data;
        }
        fclose($fh);
        return $lines;
    }

    public function assertLinesEqualsSorted($expected, $actual, $message = '')
    {
        $expected = explode("\n", $expected);
        $actual = explode("\n", $actual);

        sort($expected);
        sort($actual);
        $this->assertEquals($expected, $actual, $message);
    }

    public function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = '')
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
        return [
            // first test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'columns' => ['id', 'name', 'sex'],
                ],
                [
                    [
                        '1',
                        'martin',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'female',
                    ],
                ],
            ],
            // first test with defined operator
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'eq',
                ],
                [
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                ],
            ],
            // second test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                ],
                [
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
            // third test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'ne',
                ],
                [
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
            // fourth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                    'whereOperator' => 'ne',
                ],
                [
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                ],
            ],
            // fifth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'eq',
                ],
                [
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                ],
            ],
            // sixth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'ne',
                ],
                [
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
        ];
    }

    protected function getTestBucketId($stage = self::STAGE_IN): string
    {
        return $this->_bucketIds[$stage];
    }

    /**
     * @return array
     * @throws \Exception
     */
    protected function createWithFormDataAndWaitForEvent(Event $event, Client $sapiClient = null)
    {
        $client = null !== $sapiClient ? $sapiClient : $this->_client;

        $id = $client->createEventWithFormData($event);

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
        $fileSearchOptions = $fileSearchOptions->setQuery(sprintf('id:%s', $fileId));

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
        $alpabet = 'abcdefghijklmnopqrstvuwxyz0123456789 ';
        $randStr = '';
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
    public function deleteBranchesByPrefix(DevBranches $devBranches, $branchPrefix)
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
     * Useful with \Keboola\Test\ClientProvider\ClientProvider
     *
     * @return array
     */
    public function provideComponentsClientType()
    {
        return [
            'defaultBranch' => [ClientProvider::DEFAULT_BRANCH],
            'devBranch' => [ClientProvider::DEV_BRANCH],
        ];
    }

    /**
     * @return array
     */
    protected function getExistingBranchForTestCase(self $that)
    {
        $branchName = $this->generateDevBranchNameForDataProvider($that);
        $devBranch = new \Keboola\StorageApi\DevBranches($that->_client);

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

        return $branch;
    }

    /**
     * @return array
     */
    protected function createDevBranchForTestCase(self $that)
    {
        $branchName = $this->generateDevBranchNameForDataProvider($that);
        $devBranch = new \Keboola\StorageApi\DevBranches($that->_client);

        $this->deleteBranchesByPrefix($devBranch, $branchName);
        return $devBranch->createBranch($branchName);
    }

    public function getDefaultBranchId(self $that)
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

    /**
     * @param int $branchId
     * @return string
     */
    public function getCurrentDevBranchName($branchId)
    {
        $devBranch = new \Keboola\StorageApi\DevBranches($this->_client);
        $branchesList = $devBranch->listBranches();
        foreach ($branchesList as $branch) {
            if ($branch['id'] === $branchId) {
                /** @var string $name */
                $name = $branch['name'];
                return $name;
            }
        }

        throw new \Exception(sprintf('Branch %s not found.', $branchId));
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
     * @param string $name
     * @return string
     */
    public function generateUniqueNameForString($name)
    {
        return sha1($this->generateDescriptionForTestObject()) . '\\' . $name;
    }

    /**
     * @param string $filePath
     * @param bool $convertNulls
     * @return array<mixed>
     */
    protected function parseCsv($filePath, $convertNulls = false)
    {
        $parsed = Client::parseCsv((string) file_get_contents($filePath));
        if ($convertNulls === false) {
            return $parsed;
        }

        foreach ($parsed as &$row) {
            foreach ($row as $key => $value) {
                if ($value === 'null') {
                    $row[$key] = null;
                }
            }
        }
        unset($row);

        return $parsed;
    }

    /**
     * @return string
     */
    private function generateDevBranchNameForDataProvider(StorageApiTestCase $that)
    {
        $providedToken = $that->_client->verifyToken();
        return implode('\\', [
            __CLASS__,
            $that->getName(false),
            $that->dataName(),
            $providedToken['id'],
        ]);
    }

    /**
     * @param $params
     * @return GoogleStorageClient
     */
    public function getGcsClientClient(array $params): GoogleStorageClient
    {
        $options = [
            'credentials' => [
                'access_token' => $params['access_token'],
                'expires_in' => $params['expires_in'],
                'token_type' => $params['token_type'],
            ],
            'projectId' => $params['projectId'],
        ];

        $fetchAuthToken = new class ($options['credentials']) implements FetchAuthTokenInterface {
            private array $creds;

            public function __construct(
                array $creds
            ) {
                $this->creds = $creds;
            }

            public function fetchAuthToken(callable $httpHandler = null)
            {
                return $this->creds;
            }

            public function getCacheKey()
            {
                return '';
            }

            public function getLastReceivedToken()
            {
                return $this->creds;
            }
        };
        return new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
    }

    protected function assertTableColumnNullable(
        Metadata $m,
        string $tableId,
        string $columnName,
        bool $expectNullable
    ): void {
        $columnMetadata = $m->listColumnMetadata(sprintf('%s.%s', $tableId, $columnName));
        $nullable = array_filter($columnMetadata, static fn(array $item) => $item['key'] === 'KBC.datatype.nullable');
        $nullable = array_values($nullable)[0];
        $this->assertSame('storage', $nullable['provider']);
        $this->assertSame($expectNullable === true ? '1' : '', $nullable['value']);
    }

    protected function skipTestForBackend(
        array $backendsWhichAreSkipped,
        string $reason = '',
        ?Client $client = null
    ): void {
        if ($client === null) {
            $client = $this->_client;
        }
        $tokenData = $client->verifyToken();
        $defaultBackend = $tokenData['owner']['defaultBackend'];
        if (in_array($defaultBackend, $backendsWhichAreSkipped, true)) {
            $message = sprintf('Test skipped for backend %s', $defaultBackend);
            if ($reason !== '') {
                $message .= ': ' . $reason;
            } else {
                $message .= '.';
            }
            self::markTestSkipped($message);
        }
    }

    protected function allowTestForBackendsOnly(
        array $backendsWhichAreAllowed,
        string $reason = '',
        ?Client $client = null
    ): void {
        if ($client === null) {
            $client = $this->_client;
        }
        $tokenData = $client->verifyToken();
        $defaultBackend = $tokenData['owner']['defaultBackend'];
        if (!in_array($defaultBackend, $backendsWhichAreAllowed, true)) {
            $message = sprintf('Test is not allowed for %s backend', $defaultBackend);
            if ($reason !== '') {
                $message .= ': ' . $reason;
            } else {
                $message .= '.';
            }
            self::markTestSkipped($message);
        }
    }
}
