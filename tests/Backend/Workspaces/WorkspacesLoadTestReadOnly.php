<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

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

    public function testWorkspaceCreatedWithOrWithoutAccess(): void
    {
        // prepare workspace
        $workspace = $this->initTestWorkspace();

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        $testBucketId = $this->getTestBucketId();
        $this->_client->createTable(
            $testBucketId,
            'animals',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        assert($backend instanceof SnowflakeWorkspaceBackend);
        $db = $backend->getDb();

        $tables = $db->fetchAll(sprintf('SHOW TABLES IN SCHEMA %s', $db->quoteIdentifier($testBucketId)));
        $this->assertCount(1, $tables);
        $this->assertSame('animals', $tables[0]['name']);
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
        $tableId = $this->linkingClient->createTable(
            $sharedBucket,
            'whales',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare table in the bucket
        $this->_client->createTable(
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
        $this->_client->createTable(
            $testBucketId,
            'trains',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $db = $backend->getDb();

        $projectDatabase = $workspace['connection']['database'];

        /* I know this is brittle, but the prefix is different on different stacks
        and locally there is AFAIK no other reasonable way to get database name,
        except maybe create a workspace there */
        $sharingProjectDatabase = str_replace($projectId, $sharingProjectId, $projectDatabase);

        $quotedProjectDatabase = $db->quoteIdentifier($projectDatabase);
        $quotedSharingProjectDatabase = $db->quoteIdentifier($sharingProjectDatabase);
        $quotedTestBucketId = $db->quoteIdentifier($testBucketId);
        $quotedSharedBucketId = $db->quoteIdentifier($sharedBucketId);

        $db->query(sprintf(
            'CREATE OR REPLACE TABLE "tableFromAnimals" AS SELECT * FROM %s.%s."animals"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $db->fetchAll('SELECT * FROM "tableFromAnimals"'));

        $db->query(sprintf(
            'CREATE OR REPLACE TABLE "tableFromTrains" AS SELECT * FROM %s.%s."trains"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $db->fetchAll('SELECT * FROM "tableFromTrains"'));

        $db->query(sprintf(
            'CREATE OR REPLACE TABLE "tableFromWhales" AS SELECT * FROM %s.%s."whales"',
            $quotedSharingProjectDatabase,
            $quotedSharedBucketId
        ));
        $this->assertCount(5, $db->fetchAll('SELECT * FROM "tableFromWhales"'));
    }

    public function testCreateWorkspaceWithReadOnlyIMUnlinkUnshare(): void
    {
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
        $this->linkingClient->createTable(
            $sharedBucket,
            'whales',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare workspace
        $workspace = $this->initTestWorkspace();

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
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
    }

    private function assertCannotAccessLinkedBucket(
        Connection $db,
        string $sharingProjectDatabase,
        string $sharedBucketId
    ): void {
        $quotedSharingProjectDatabase = $db->quoteIdentifier($sharingProjectDatabase);
        $quotedSharedBucketId = $db->quoteIdentifier($sharedBucketId);

        try {
            $db->fetchAll(
                sprintf(
                    'SELECT * FROM %s.%s."whales"',
                    $quotedSharingProjectDatabase,
                    $quotedSharedBucketId
                )
            );
            $this->fail('should fail');
        } catch (\Exception $e) {
            $this->assertEquals(sprintf("odbc_prepare(): SQL error: SQL compilation error:
Schema '%s.%s' does not exist or not authorized., SQL state 02000 in SQLPrepare", $sharingProjectDatabase, $quotedSharedBucketId), $e->getMessage());
        }
    }

    private function assertLoadDataFromLinkedBucket(
        Connection $db,
        string $sharingProjectDatabase,
        string $sharedBucketId
    ): void {
        $quotedSharingProjectDatabase = $db->quoteIdentifier($sharingProjectDatabase);
        $quotedSharedBucketId = $db->quoteIdentifier($sharedBucketId);

        $this->assertCount(
            5,
            $db->fetchAll(
                sprintf('SELECT * FROM %s.%s."whales"', $quotedSharingProjectDatabase, $quotedSharedBucketId)
            )
        );
    }
}
