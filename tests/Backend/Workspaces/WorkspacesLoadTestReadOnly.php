<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Throwable;

class WorkspacesLoadTestReadOnly extends ParallelWorkspacesTestCase
{
    private Client $linkingClient;

    public function setUp(): void
    {
        parent::setUp();
        $token = $this->_client->verifyToken();
        if (!in_array('input-mapping-read-only-storage', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Read only mapping is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->linkingClient = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );

        $tokenLinking = $this->linkingClient->verifyToken();
        if (!in_array('input-mapping-read-only-storage', $tokenLinking['owner']['features'])) {
            $this->markTestSkipped(sprintf(
                'Read only mapping is not enabled for project "%s"',
                $tokenLinking['owner']['id']
            ));
        }
    }

    /**
     * @dataProvider provideDataForWorkspaceCreation
     */
    public function testWorkspaceCreatedWithOrWithoutAccess(?bool $roParameter, bool $shouldHaveRo): void
    {
        // prepare workspace
        $options = [];
        if ($roParameter !== null) {
            $options['readOnlyStorageAccess'] = $roParameter;
        }
        $workspace = $this->initTestWorkspace(null, $options, true);

        $this->assertSame($shouldHaveRo, $workspace['readOnlyStorageAccess']);

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        $testBucketId = $this->getTestBucketId();
        $this->_client->createTableAsync(
            $testBucketId,
            'animals',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        assert($backend instanceof SnowflakeWorkspaceBackend);
        $db = $backend->getDb();

        if ($shouldHaveRo) {
            $tables = $db->fetchAll(sprintf('SHOW TABLES IN SCHEMA %s', SnowflakeQuote::quoteSingleIdentifier($testBucketId)));
            $this->assertCount(1, $tables);
            $this->assertSame('animals', $tables[0]['name']);
        } else {
            try {
                $db->fetchAll(sprintf('SHOW TABLES IN SCHEMA %s', SnowflakeQuote::quoteSingleIdentifier($testBucketId)));
                $this->fail('Should have failed');
            } catch (Throwable $e) {
                $this->assertSame(
                    'odbc_prepare(): SQL error: SQL compilation error:
Object does not exist, or operation cannot be performed., SQL state 02000 in SQLPrepare',
                    $e->getMessage(),
                    'Workspace should not have access to storage'
                );
            }
        }
    }

    public function testCreateWorkspaceWithReadOnlyIM(): void
    {
        // prepare bucket
        $testBucketId = $this->getTestBucketId();
        $testBucketName = str_replace('in.c-', '', $testBucketId);

        $sharedBucketName = $this->getTestBucketName($this->generateDescriptionForTestObject() . '-sharedBucket');
        $linkedBucketName = $this->getTestBucketName($this->generateDescriptionForTestObject() . '-linkedBucket');
        $sharedBucket = 'in.c-' . $sharedBucketName;
        $linkedBucketId = 'in.c-' . $linkedBucketName;
        $this->dropBucketIfExists($this->_client, $linkedBucketId, true);
        $this->dropBucketIfExists($this->_client, $testBucketId, true);
        $this->dropBucketIfExists($this->linkingClient, $sharedBucket, true);

        $sharedBucketId = $this->linkingClient->createBucket($sharedBucketName, 'in');
        $this->linkingClient->shareOrganizationBucket($sharedBucketId, true);
        $sharingToken = $this->linkingClient->verifyToken();
        $token = $this->_client->verifyToken();
        $sharingProjectId = $sharingToken['owner']['id'];
        $projectId = $token['owner']['id'];
        $this->_client->linkBucket($linkedBucketName, 'in', $sharingProjectId, $sharedBucketId, null, true);

        $testBucketId = $this->_client->createBucket($testBucketName, 'in');

        //setup test tables
        $sharedTableId = $this->linkingClient->createTableAsync(
            $sharedBucket,
            'whales',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare table in the bucket
        $this->_client->createTableAsync(
            $testBucketId,
            'animals',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare workspace
        $workspace = $this->initTestWorkspace();

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        // prepare table in the bucket created after workspace created
        $this->_client->createTableAsync(
            $testBucketId,
            'trains',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $projectDatabase = $workspace['connection']['database'];

        /* I know this is brittle, but the prefix is different on different stacks
        and locally there is AFAIK no other reasonable way to get database name,
        except maybe create a workspace there */
        $sharingProjectDatabase = str_replace($projectId, $sharingProjectId, $projectDatabase);

        $quotedProjectDatabase = SnowflakeQuote::quoteSingleIdentifier($projectDatabase);
        $quotedSharingProjectDatabase = SnowflakeQuote::quoteSingleIdentifier($sharingProjectDatabase);
        $quotedTestBucketId = SnowflakeQuote::quoteSingleIdentifier($testBucketId);
        $quotedSharedBucketId = SnowflakeQuote::quoteSingleIdentifier($sharedBucketId);

        $db = $backend->getDb();
        $db->executeStatement(sprintf(
            'CREATE OR REPLACE TABLE "tableFromAnimals" AS SELECT * FROM %s.%s."animals"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $backend->fetchAll('tableFromAnimals'));

        $db->executeStatement(sprintf(
            'CREATE OR REPLACE TABLE "tableFromTrains" AS SELECT * FROM %s.%s."trains"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $backend->fetchAll('tableFromTrains'));

        $db->executeStatement(sprintf(
            'CREATE OR REPLACE TABLE "tableFromWhales" AS SELECT * FROM %s.%s."whales"',
            $quotedSharingProjectDatabase,
            $quotedSharedBucketId
        ));
        $this->assertCount(5, $backend->fetchAll('tableFromWhales'));

        //test load using view
        $options = [
            'input' => [
                [
                    'source' => str_replace($sharedBucket, $linkedBucketId, $sharedTableId),
                    'destination' => 'testRO',
                    'useView' => true,
                ],
            ],
        ];
        $workspaces = new Workspaces($this->_client);
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $tableRef = $backend->getTableReflection('testRO');
        $this->assertTrue($tableRef->isView());
        $this->assertCount(5, $backend->fetchAll('testRO'));
    }

    public function testCreateWorkspaceWithReadOnlyIMUnlinkUnshare(): void
    {
        assert($this->linkingClient !== null);
        $tokenConsumer = $this->_client->verifyToken();
        $consumerProjectId = $tokenConsumer['owner']['id'];

        // aka linking token
        $tokenProducer = $this->linkingClient->verifyToken();
        $sharingProjectId = $tokenProducer['owner']['id'];

        $sharedBucketName = $this->getTestBucketName($this->generateDescriptionForTestObject() . '-sharedBucket');
        $linkedBucketName = $this->getTestBucketName($this->generateDescriptionForTestObject() . '-linkedBucket');
        $sharedBucket = 'in.c-' . $sharedBucketName;
        $linkedBucketId = 'in.c-' . $linkedBucketName;
        $this->dropBucketIfExists($this->_client, $linkedBucketId, true);
        $this->dropBucketIfExists($this->linkingClient, $sharedBucket, true);

        $sharedBucketId = $this->linkingClient->createBucket($sharedBucketName, 'in');
        $this->linkingClient->shareBucketToProjects($sharedBucketId, [$consumerProjectId], true);

        $this->_client->linkBucket($linkedBucketName, 'in', $sharingProjectId, $sharedBucketId, null, false);

        //setup test tables
        $sharedTableId = $this->linkingClient->createTableAsync(
            $sharedBucket,
            'whales',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare workspace
        $workspace = $this->initTestWorkspace();

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);
        $workspaces = new Workspaces($this->_client);
        // load table as view
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => str_replace($sharedBucket, $linkedBucketId, $sharedTableId),
                        'destination' => 'testRO',
                        'useView' => true,
                    ],
                ],
            ]
        );

        $db = $backend->getDb();
        $projectDatabase = $workspace['connection']['database'];
        $sharingProjectDatabase = str_replace($consumerProjectId, $sharingProjectId, $projectDatabase);

        // unlink
        $this->_client->dropBucket($linkedBucketId, ['async' => true]);
        $this->assertCannotAccessLinkedBucket($db, $sharingProjectDatabase, $sharedBucketId);
        // link again
        $this->_client->linkBucket($linkedBucketName, 'in', $sharingProjectId, $sharedBucketId, null, false);
        // check that the sharing still works
        $this->assertLoadDataFromLinkedBucket($db, $sharingProjectDatabase, $sharedBucketId);

        assert($this->linkingClient !== null);
        // unshare (unlink first)
        $this->_client->dropBucket($linkedBucketId, ['async' => true]);
        $this->linkingClient->unshareBucket($sharedBucketId, true);
        $this->assertCannotAccessLinkedBucket($db, $sharingProjectDatabase, $sharedBucketId);

        // force unlink
        assert($this->linkingClient !== null);
        $this->linkingClient->shareBucketToProjects($sharedBucketId, [$consumerProjectId], true);
        $this->_client->linkBucket($linkedBucketName, 'in', $sharingProjectId, $sharedBucketId, null, false);
        // check that the sharing still works
        $this->assertLoadDataFromLinkedBucket($db, $sharingProjectDatabase, $sharedBucketId);
        assert($this->linkingClient !== null);
        $this->linkingClient->forceUnlinkBucket($sharedBucketId, $consumerProjectId, ['async' => true]);
        $this->assertCannotAccessLinkedBucket($db, $sharingProjectDatabase, $sharedBucketId);

        $tableRef = $backend->getTableReflection('testRO');
        $this->assertTrue($tableRef->isView());
        try {
            $backend->fetchAll('testRO');
        } catch (Throwable $e) {
            $this->assertStringContainsString('does not exist or not authorized', $e->getMessage());
        }
    }

    private function assertCannotAccessLinkedBucket(
        \Doctrine\DBAL\Connection $db,
        string $sharingProjectDatabase,
        string $sharedBucketId
    ): void {
        $quotedSharingProjectDatabase = SnowflakeQuote::quoteSingleIdentifier($sharingProjectDatabase);
        $quotedSharedBucketId = SnowflakeQuote::quoteSingleIdentifier($sharedBucketId);

        try {
            $db->fetchAllAssociative(
                sprintf(
                    'SELECT * FROM %s.%s."whales"',
                    $quotedSharingProjectDatabase,
                    $quotedSharedBucketId
                )
            );
            $this->fail('should fail');
        } catch (\Exception $e) {
            $this->assertEquals(sprintf("An exception occurred while executing a query: SQL compilation error:
Schema '%s.%s' does not exist or not authorized.", $sharingProjectDatabase, $quotedSharedBucketId), $e->getMessage());
        }
    }

    private function assertLoadDataFromLinkedBucket(
        \Doctrine\DBAL\Connection $db,
        string $sharingProjectDatabase,
        string $sharedBucketId
    ): void {
        $quotedSharingProjectDatabase = SnowflakeQuote::quoteSingleIdentifier($sharingProjectDatabase);
        $quotedSharedBucketId = SnowflakeQuote::quoteSingleIdentifier($sharedBucketId);

        $this->assertCount(
            5,
            $db->fetchAllAssociative(
                sprintf('SELECT * FROM %s.%s."whales"', $quotedSharingProjectDatabase, $quotedSharedBucketId)
            )
        );
    }

    public function provideDataForWorkspaceCreation(): \Generator
    {
        yield 'asked for disabled, gets disabled' => [
            false,
            false,
        ];
        yield 'asked for enabled, gets enabled' => [
            true,
            true,
        ];
        yield 'nothing provided, gets enabled because feature is on' => [
            null,
            true,
        ];
//        // for local test purposes
//        yield 'nothing provided, gets disabled because feature is off' => [
//            null,
//            false,
//        ];
    }
}
