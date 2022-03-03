<?php

namespace Keboola\Test\Backend\MixedSnowflakeSynapse;

use Doctrine\DBAL\Connection;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\Test\Backend\Mixed\StorageApiSharingTestCase;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\SynapseWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SharingTest extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;

    public function sharingBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SYNAPSE],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [
                'sharing backend' => self::BACKEND_SNOWFLAKE,
                'workspace backend' => self::BACKEND_SYNAPSE,
                'load type' => 'staging',
            ],
            [
                'sharing backend' => self::BACKEND_SYNAPSE,
                'workspace backend' => self::BACKEND_SYNAPSE,
                'load type' => 'direct',
            ],
        ];
    }

    public function testOrganizationAdminInTokenVerify()
    {
        $token = $this->_client->verifyToken();
        $this->assertTrue($token['admin']['isOrganizationMember']);
    }

    /**
     * @dataProvider workspaceMixedBackendData
     *
     * @param string $sharingBackend
     * @param string $workspaceBackend
     * @param string $expectedLoadType
     * @throws ClientException
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Keboola\StorageApi\Exception
     */
    public function testWorkspaceLoadData(
        $sharingBackend,
        $workspaceBackend,
        $expectedLoadType
    ) {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets($sharingBackend);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $secondBucketId = $this->getTestBucketId(self::STAGE_OUT);
        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'name',
            ]
        );
        $table1SecondBucketId = $this->_client->createTableAsync(
            $secondBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'name',
            ]
        );
        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)) {
            $this->assertExpectedDistributionKeyColumn($table1Id, 'name');
        }

        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv'),
            [
                'primaryKey' => '1',
            ]
        );
        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)) {
            $this->assertExpectedDistributionKeyColumn($table2Id, '1');
        }

        $table3Id = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'numbers-alias'
        );
        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)) {
            $this->assertExpectedDistributionKeyColumn($table3Id, '1');
        }
        // share and link bucket
        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedId = $this->_client2->linkBucket(
            "linked-" . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );
        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)) {
            $tables = $this->_client2->listTables($linkedId);
            foreach ($tables as $table) {
                switch ($table['sourceTable']) {
                    case $table1Id:
                        $this->assertExpectedDistributionKeyColumn($table1Id, 'name');
                        break;
                    case $table2Id:
                    case $table3Id:
                        $this->assertExpectedDistributionKeyColumn($table3Id, '1');
                        break;
                }
            }
        }

        // share and unshare second bucket - test that it doesn't break permissions of first linked bucket
        $this->_client->shareBucket($secondBucketId);
        $sharedBucket2 = array_values(array_filter($this->_client->listSharedBuckets(), function ($bucket) use ($secondBucketId) {
            return $bucket['id'] === $secondBucketId;
        }))[0];
        $linked2Id = $this->_client2->linkBucket(
            "linked-2-" . time(),
            'out',
            $sharedBucket2['project']['id'],
            $sharedBucket2['id']
        );
        $this->_client2->dropBucket($linked2Id);


        $mapping1 = array(
            "source" => str_replace($bucketId, $linkedId, $table1Id),
            "destination" => "languagesLoaded"
        );

        $mapping2 = array(
            "source" => str_replace($bucketId, $linkedId, $table2Id),
            "destination" => "numbersLoaded"
        );

        $mapping3 = array(
            "source" => str_replace($bucketId, $linkedId, $table3Id),
            "destination" => "numbersAliasLoaded"
        );

        // init workspace
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace([
            "backend" => $workspaceBackend
        ]);

        $input = array($mapping1, $mapping2, $mapping3);

        // test if job is created and listed
        $initialJobs = $this->_client2->listJobs();

        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));

        $this->createAndWaitForEvent(
            (new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'),
            $this->_client2
        );

        $events = $this->_client2->listEvents(['runId' => $runId, 'q' => 'storage.workspaceLoaded',]);
        $this->assertCount(3, $events);
        foreach ($events as $event) {
            $this->assertSame($expectedLoadType, $event['results']['loadType']);
        }

        $afterJobs = $this->_client2->listJobs();


        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals(empty($initialJobs) ? 0 : $initialJobs[0]['id'], $afterJobs[0]['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client2->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(3, $export['totalCount']);
        $this->assertCount(3, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersAliasLoaded"), $tables);

        // check table structure and data
        $data = $backend->fetchAll("languagesLoaded", \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'),
            $data,
            'id'
        );

        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)
            && $backend instanceof SynapseWorkspaceBackend
        ) {
            $ref = $backend->getTableReflection('languagesLoaded');
            self::assertEquals('HASH', $ref->getTableDistribution());
            self::assertSame(['name'], $ref->getTableDistributionColumnsNames());
            $ref = $backend->getTableReflection('numbersLoaded');
            self::assertEquals('HASH', $ref->getTableDistribution());
            self::assertSame(['1'], $ref->getTableDistributionColumnsNames());
            $ref = $backend->getTableReflection('numbersAliasLoaded');
            self::assertEquals('HASH', $ref->getTableDistribution());
            self::assertSame(['1'], $ref->getTableDistributionColumnsNames());
        }

        // now we'll load another table and use the preserve parameters to check that all tables are present
        // lets create it now to see if the table permissions are correctly propagated
        $table3Id = $this->_client->createTable(
            $bucketId,
            'numbersLater',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $mapping3 = array("source" => str_replace($bucketId, $linkedId, $table3Id), "destination" => "table3");
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3), "preserve" => true));

        $this->createAndWaitForEvent(
            (new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'),
            $this->_client2
        );

        $events = $this->_client2->listEvents(['runId' => $runId, 'q' => 'storage.workspaceLoaded',]);
        $this->assertCount(1, $events);
        $this->assertSame($expectedLoadType, $events[0]['results']['loadType']);

        $tables = $backend->getTables();

        $this->assertCount(4, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersAliasLoaded"), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));

        $this->createAndWaitForEvent(
            (new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'),
            $this->_client2
        );

        $events = $this->_client2->listEvents(['runId' => $runId, 'q' => 'storage.workspaceLoaded',]);
        $this->assertCount(1, $events);
        $this->assertSame($expectedLoadType, $events[0]['results']['loadType']);

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);

        $unloadBucketId = 'out.c-sharing-test-unload';
        try {
            $this->_client2->getBucket($unloadBucketId);
            // if bucket exists drop it
            $this->_client2->dropBucket($unloadBucketId, ['force' => true]);
        } catch (ClientException $e) {
            $this->assertEquals("Bucket {$unloadBucketId} not found", $e->getMessage());
        } finally {
            // create unload bucket
            $this->_client2->createBucket("sharing-test-unload", "out", "", 'synapse');
        }

        // now try load as view
        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)
            && $backend instanceof SynapseWorkspaceBackend
        ) {
            $inputAsView = [
                [
                    "source" => str_replace($bucketId, $linkedId, $table1Id),
                    "destination" => "languagesLoaded",
                    "useView" => true,
                ],

                [
                    "source" => str_replace($bucketId, $linkedId, $table2Id),
                    "destination" => "numbersLoaded",
                    "useView" => true,
                ],

                [
                    "source" => str_replace($bucketId, $linkedId, $table3Id),
                    "destination" => "numbersAliasLoaded",
                    "useView" => true,
                ],
            ];

            $workspaces->loadWorkspaceData($workspace['id'], ["input" => $inputAsView]);

            // check that the tables are in the workspace
            $views = ($backend->getSchemaReflection())->getViewsNames();
            self::assertCount(3, $views);
            self::assertCount(0, $backend->getTables());
            self::assertContains($backend->toIdentifier("languagesLoaded"), $views);
            self::assertContains($backend->toIdentifier("numbersLoaded"), $views);
            self::assertContains($backend->toIdentifier("numbersAliasLoaded"), $views);

            // check table structure and data
            $data = $backend->fetchAll("languagesLoaded", \PDO::FETCH_ASSOC);
            self::assertCount(5, $data, 'there should be 5 rows');
            self::assertCount(3, $data[0], 'there should be two columns');
            self::assertArrayHasKey('id', $data[0]);
            self::assertArrayHasKey('name', $data[0]);
            self::assertArrayHasKey('_timestamp', $data[0]);

            $connection = $workspace['connection'];
            /** @var Connection $db */
            $db = $this->getDbConnection($connection);
            // User in Synapse can create view from table and scheme where he don't have permission to access
            // this test can't be run in regular test suite as we don't know schema name of the bucket
            // if desired to run uncoment and fill real schema name
            //$db->executeStatement('CREATE VIEW [test_transit_load] AS SELECT [id],[name] FROM [DEV_ZAJCA_1621-out_c-API-sharing].[languages]');
            //try {
            //    $db->fetchAll('SELECT * FROM [test_transit_load]');
            //    $this->fail('Must fail as workspace user can\'t select from source table');
            //} catch (\Exception $e) {
            //    var_export($e->getMessage());
            //}
            //
            //try {
            //    $db->fetchAll('CREATE TABLE [test_ctas] WITH (DISTRIBUTION = ROUND_ROBIN) AS SELECT * FROM [test_transit_load]');
            //    $this->fail('Must fail as workspace user can\'t select from source table');
            //} catch (\Exception $e) {
            //    var_export($e->getMessage());
            //}
            //
            //try {
            //    $this->_client2->createTableAsyncDirect($unloadBucketId, [
            //        'name' => 'should-fail-on-credentials',
            //        'dataWorkspaceId' => $workspace['id'],
            //        'dataTableName' => 'test_transit_load',
            //    ]);
            //    $this->fail('Must fail as project credentials can\'t select from source table of view');
            //} catch (ClientException $e) {
            //    var_export($e->getMessage());
            //}

            //create view without _timestamp column
            $db->executeStatement('CREATE VIEW [test_load_from_view] AS SELECT [id],[name] FROM [languagesLoaded]');
            // must work unload from view
            $this->_client2->createTableAsyncDirect($unloadBucketId, [
                'name' => 'languagesUnLoaded',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test_load_from_view',
            ]);
        }

        // unload validation
        $connection = $workspace['connection'];

        $backend = null; // force disconnect of same SNFLK connection
        $db = $this->getDbConnection($connection);

        if ($db instanceof \Doctrine\DBAL\Connection) {
            $db->query("create table [Languages3] (
			[Id] integer not null,
			[Name] varchar(10) not null
		);");
            $db->query("insert into [Languages3] ([Id], [Name]) values (1, 'cz');");
            $db->query("insert into [Languages3] ([Id], [Name]) values (2, 'en');");
        } else {
            $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
            $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        }
        try {
            $this->_client2->createTableAsyncDirect($linkedId, array(
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'Languages3',
            ));

            $this->fail('Unload to liked bucket should fail with access exception');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        // must work unload from workspace table
        $this->_client2->createTableAsyncDirect($unloadBucketId, [
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'languages3',
        ]);

        // drop second bucket without linking
        $this->_client->unshareBucket($secondBucketId);
        $this->_client->dropBucket($secondBucketId, ['force' => true]);

        // drop first bucket
        try {
            $this->_client->dropBucket($bucketId, ['force' => true]);
            $this->fail('Bucket must not be dropped as it\'s linked in other project.');
        } catch (ClientException $e) {
            $this->assertSame('The bucket is already linked in other projects.', $e->getMessage());
            $this->assertSame('storage.buckets.alreadyLinked', $e->getStringCode());
        }
        // force unlink bucket from project
        $projectId = $this->_client2->verifyToken()['owner']['id'];
        $this->_client->forceUnlinkBucket($bucketId, $projectId, ['async' => true]);

        if ($this->isSynapseTestCase($sharingBackend, $workspaceBackend)
            && $db instanceof Connection
        ) {
            $schemaRef = (new SynapseSchemaReflection($db, $connection['schema']));
            // test number of views after source unlink
            $views = $schemaRef->getViewsNames();
            // there is one view test_load_from_view which is not refreshed but lost binding
            self::assertCount(1, $views);
        }

        $this->_client->dropBucket($bucketId, ['force' => true]);
    }

    /**
     * @param string $sharingBackend
     * @param string $workspaceBackend
     * @return bool
     */
    private function isSynapseTestCase(
        $sharingBackend,
        $workspaceBackend
    ) {
        return $sharingBackend === self::BACKEND_SYNAPSE && $workspaceBackend === self::BACKEND_SYNAPSE;
    }

    /**
     * @param string $tableId
     * @param string $columnName
     */
    private function assertExpectedDistributionKeyColumn($tableId, $columnName)
    {
        $table = $this->_client->getTable($tableId);
        self::assertSame([$columnName], $table['distributionKey']);
    }
}
