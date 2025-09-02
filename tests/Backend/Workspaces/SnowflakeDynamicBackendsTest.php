<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;

class SnowflakeDynamicBackendsTest extends ParallelWorkspacesTestCase
{
    const SOURCE_DATA_FILE_PATH = __DIR__ . '/../../_data/languages.csv';
    const WORKSPACE_TABLE_NAME = 'languagesLoaded';

    /** @var Client */
    private $client2;

    /** @var Workspaces */
    private $workspaces;

    /** @var Workspaces */
    private $workspaces2;

    public function setUp(): void
    {
        parent::setUp();

        $token = $this->_client->verifyToken();
        if (!in_array('workspace-snowflake-dynamic-backend-size', $token['owner']['features'])) {
            $this->fail(sprintf('Dynamic backends are not enabled for project "%s"', $token['owner']['id']));
        }

        $this->client2 = $this->getClientForToken(STORAGE_API_LINKING_TOKEN);

        $token2 = $this->client2->verifyToken();
        if (in_array('workspace-snowflake-dynamic-backend-size', $token2['owner']['features'])) {
            $this->fail(sprintf('Dynamic backends should be disabled for project "%s"', $token2['owner']['id']));
        }

        $this->deleteOldTestWorkspaces();

        $this->workspaces = new Workspaces($this->workspaceSapiClient);

        $this->workspaces2 = new Workspaces($this->getClient([
            'token' => $this->initTestToken(new Tokens($this->client2)),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]));

        foreach ($this->listTestWorkspaces($this->client2) as $workspace) {
            $this->workspaces2->deleteWorkspace($workspace['id'], [], true);
        }

        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider workspaceCreateData
     */
    public function testWorkspaceCreate(
        $backendSize,
        $expectedBackendSize,
        $expectedWarehouseSuffix
    ): void {
        $workspace = $this->workspaces->createWorkspace(
            [
                'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
                'backendSize' => $backendSize,
            ],
            true,
        );

        // Check queries against warehouse - no exception expected
        $db = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        // -- Pick the warehouse you want to test
        $db->executeQuery("USE WAREHOUSE {$workspace['connection']['warehouse']};");
        // -- Ensure it actually spins compute (no result cache)
        $db->executeQuery('ALTER SESSION SET USE_CACHED_RESULT = FALSE;');
        // -- Easiest dummy SELECT that runs on the warehouse
        $db->executeQuery('SELECT CURRENT_WAREHOUSE(), CURRENT_TIMESTAMP(), RANDOM();');

        $this->assertSame('snowflake', $workspace['connection']['backend']);
        $this->assertSame($expectedBackendSize, $workspace['backendSize']);
        $this->assertStringEndsWith($expectedWarehouseSuffix, $workspace['connection']['warehouse']);

        $credentials = $this->workspaces->createCredentials($workspace['id']);

        // Check queries against warehouse - no exception expected
        $db2 = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($credentials);
        // -- Pick the warehouse you want to test
        $db2->executeQuery("USE WAREHOUSE {$credentials['connection']['warehouse']};");
        // -- Ensure it actually spins compute (no result cache)
        $db2->executeQuery('ALTER SESSION SET USE_CACHED_RESULT = FALSE;');
        // -- Easiest dummy SELECT that runs on the warehouse
        $db2->executeQuery('SELECT CURRENT_WAREHOUSE(), CURRENT_TIMESTAMP(), RANDOM();');

        $this->assertSame('snowflake', $credentials['connection']['backend']);
        $this->assertSame($expectedBackendSize, $credentials['backendSize']);
        $this->assertStringEndsWith($expectedWarehouseSuffix, $credentials['connection']['warehouse']);
    }

    public function testWorkspaceLoadLinkedDataFromProjectWithDynamicBackends(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::SOURCE_DATA_FILE_PATH),
        );

        $this->_client->shareOrganizationProjectBucket($bucketId);

        $workspace = $this->workspaces2->createWorkspace(
            [
                'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
            ],
            true,
        );

        $this->assertNotNull($workspace['backendSize']);

        $sharedBuckets = array_filter(
            $this->client2->listSharedBuckets(),
            function ($sharedBucket) use ($bucketId) {
                return $bucketId === $sharedBucket['id'];
            },
        );

        $this->assertCount(1, $sharedBuckets);
        $sharedBucket = reset($sharedBuckets);

        $linkedBucketId = $this->client2->linkBucket(
            $this->getTestBucketName($this->generateDescriptionForTestObject()) . '-linked',
            self::STAGE_IN,
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertCount(0, $backend->getTables());

        $this->workspaces2->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $linkedBucketId . '.languages',
                        'destination' => self::WORKSPACE_TABLE_NAME,
                    ],
                ],
            ],
        );

        $this->assertDataInWorkspace($backend);
    }

    public function testWorkspaceLoadLinkedDataFromProjectWithoutDynamicBackends(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $bucketName = $this->getTestBucketName($description);

        $bucketId = $this->initEmptyBucket(
            $bucketName,
            self::STAGE_IN,
            $description,
            $this->client2,
        );

        $this->client2->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::SOURCE_DATA_FILE_PATH),
        );

        $this->client2->shareOrganizationProjectBucket($bucketId);

        $workspace = $this->workspaces->createWorkspace(
            [
                'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
                'backendSize' => 'testsize',
            ],
            true,
        );

        $this->assertSame('testsize', $workspace['backendSize']);

        $sharedBuckets = array_filter(
            $this->_client->listSharedBuckets(),
            function ($sharedBucket) use ($bucketId) {
                return $bucketId === $sharedBucket['id'];
            },
        );
        $this->assertCount(1, $sharedBuckets);
        $sharedBucket = reset($sharedBuckets);

        $linkedBucketId = $this->_client->linkBucket(
            $bucketName . '-linked',
            self::STAGE_IN,
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertCount(0, $backend->getTables());

        $this->workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $linkedBucketId . '.languages',
                        'destination' => self::WORKSPACE_TABLE_NAME,
                    ],
                ],
            ],
        );

        $this->assertDataInWorkspace($backend);
    }

    public function testWorkspaceCreateError(): void
    {
        try {
            $this->workspaces->createWorkspace([
                'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
                'backendSize' => 'ultralarge',
            ]);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertMatchesRegularExpression(
                '/^Invalid backend size: "ultralarge". Allowed values: [a-z\,\ ]+\.$/',
                $e->getMessage(),
            );
            $this->assertSame('workspace.unsupportedBackendSize', $e->getStringCode());
        }
    }

    /**
     * @dataProvider workspaceCreateRequestObjectErrorData
     */
    public function testWorkspaceCreateRequestObjectError(
        array $params,
        array $expectedErrors
    ): void {
        try {
            $this->workspaces->createWorkspace($params, true);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertStringStartsWith('Invalid request', $e->getMessage());
            $this->assertSame($expectedErrors, $e->getContextParams()['errors']);
            $this->assertSame('validation.failed', $e->getStringCode());
        }
    }

    public function testWorkspaceLoadData(): void
    {
        $workspaces = $this->workspaces;

        $workspace = $workspaces->createWorkspace([
            'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
            'backendSize' => 'testsize',
        ]);

        $this->assertSame('testsize', $workspace['backendSize']);

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(self::SOURCE_DATA_FILE_PATH),
        );

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $this->assertCount(0, $backend->getTables());

        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => self::WORKSPACE_TABLE_NAME,
                    ],
                ],
            ],
        );

        $this->assertDataInWorkspace($backend);
    }

    public function workspaceCreateData()
    {
        return [
            'without backend size' => [
                null,
                'small',
                '_SMALL',
            ],
            'small size' => [
                'small',
                'small',
                '_SMALL',
            ],
            'large size' => [
                'testsize',
                'testsize',
                '_TESTSIZE',
            ],
        ];
    }

    public function workspaceCreateRequestObjectErrorData()
    {
        return [
            'backendSize without backend' => [
                [
                    'backendSize' => 'xlarge',
                ],
                [
                    [
                        'key' => 'backendSize',
                        'message' => 'Cannot use "backendSize" parameter without "backend" specified.',
                    ],
                ],
            ],
            'backendSize with invalid backend combination' => [
                [
                    'backendSize' => 'xlarge',
                    'backend' => 'synapse',
                ],
                [
                    [
                        'key' => 'backendSize',
                        'message' => 'Parameter "backendSize" is supported only for "snowflake" backend.',
                    ],
                ],
            ],
        ];
    }

    private function assertDataInWorkspace(SnowflakeWorkspaceBackend $backend)
    {
        // check that the tables are in the workspace
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier(self::WORKSPACE_TABLE_NAME), $tables);

        // check table structure and data
        $data = $backend->fetchAll(self::WORKSPACE_TABLE_NAME, \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(
                file_get_contents(self::SOURCE_DATA_FILE_PATH),
            ),
            $data,
            'id',
        );
    }

    public function testImportToStorageFromWorkspaceWithDynamicBackend(): void
    {
        // trying to add columns on the fly on SNFLK "typed" table
        $tableDefinition = [
            'name' => 'MOJE_SUPERKULATOUCKA_TABULECKA',
            'primaryKeysNames' => ['mujesuperhranateid'],
            'columns' => [
                [
                    'name' => 'mujesuperhranateid',
                    'basetype' => 'INTEGER',
                ],
                [
                    'name' => 'name',
                    'basetype' => 'STRING',
                ],
            ],
        ];

        $typedTableId = $this->_client->createTableDefinition($this->getTestBucketId(), $tableDefinition);

        $workspaces = $this->workspaces;

        // 1. Create workspace with dynamic backend size 'testsize'
        $workspace = $workspaces->createWorkspace([
            'backend' => StorageApiTestCase::BACKEND_SNOWFLAKE,
            'backendSize' => 'testsize',
        ]);
        $this->assertSame('testsize', $workspace['backendSize']);

        // 2. Prepare table in workspace
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $tableName = 'MOJE_SUPERKULATOUCKA_TABULECKA';
        $backend->createTable($tableName, ['mujesuperhranateid' => 'INTEGER', 'name' => 'VARCHAR']);
        $this->assertCount(1, $backend->getTables());
        $backend->executeQuery("INSERT INTO MOJE_SUPERKULATOUCKA_TABULECKA VALUES (1, 'martin');");

        // 3. Import table from workspace to storage
        $this->_client->writeTableAsyncDirect($typedTableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'MOJE_SUPERKULATOUCKA_TABULECKA',
            'incremental' => true,
        ]);
        // There is no way to assert the warehouse used for import.
        // You can only check it in the UI in Snowflake's QueryHistory or in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.
        // You should see the warehouse with suffix "_TESTSIZE" in the name.
    }
}
