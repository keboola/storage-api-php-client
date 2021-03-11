<?php

namespace Keboola\Test\Backend\MixedSnowflakeSynapse;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
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
                'workspace backend' => self::BACKEND_SNOWFLAKE,
                'load type' => 'direct',
            ],
            [
                'sharing backend' => self::BACKEND_SNOWFLAKE,
                'workspace backend' => self::BACKEND_SYNAPSE,
                'load type' => 'staging',
            ],
            //[
            //    'sharing backend' => self::BACKEND_SYNAPSE,
            //    'workspace backend' => self::BACKEND_SNOWFLAKE,
            //    'load type' => 'staging',
            //],
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
