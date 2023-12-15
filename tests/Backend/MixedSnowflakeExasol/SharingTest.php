<?php

namespace Keboola\Test\Backend\MixedSnowflakeExasol;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Mixed\StorageApiSharingTestCase;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class SharingTest extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;

    /**
     * @return array[]
     */
    public function sharingBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_EXASOL],
        ];
    }

    /**
     * @return array[]
     */
    public function workspaceMixedBackendData()
    {
        return [
            'exa to snowflake' =>
                [
                    'sharing backend' => self::BACKEND_SNOWFLAKE,
                    'workspace backend' => self::BACKEND_EXASOL,
                    'load type' => 'staging',
                ],
            'exa to exa' =>
                [
                    'sharing backend' => self::BACKEND_EXASOL,
                    'workspace backend' => self::BACKEND_EXASOL,
                    'load type' => 'direct',
                ],
        ];
    }

    /**
     * @dataProvider workspaceMixedBackendData
     *
     * @param string $sharingBackend
     * @param string $workspaceBackend
     * @param string $expectedLoadType
     * @return void
     * @throws ClientException
     * @throws \Doctrine\DBAL\Exception
     * @throws \Keboola\StorageApi\Exception
     */
    public function testWorkspaceLoadData(
        $sharingBackend,
        $workspaceBackend,
        $expectedLoadType
    ): void {
        $this->initEvents($this->_client2);

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
            ],
        );

        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv'),
            [
                'primaryKey' => '1',
            ],
        );

        $table3Id = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'numbers-alias',
        );

        // share and link bucket
        $this->_client->shareBucket($bucketId);
        self::assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        /** @var string $linkedId */
        $linkedId = $this->_client2->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );

        // share and unshare second bucket - test that it doesn't break permissions of first linked bucket
        $this->_client->shareBucket($secondBucketId);
        $sharedBucket2 = array_values(array_filter($this->_client->listSharedBuckets(), function ($bucket) use (
            $secondBucketId
        ) {
            return $bucket['id'] === $secondBucketId;
        }))[0];
        /** @var string $linked2Id */
        $linked2Id = $this->_client2->linkBucket(
            'linked-2-' . time(),
            'out',
            $sharedBucket2['project']['id'],
            $sharedBucket2['id'],
        );
        $this->_client2->dropBucket($linked2Id, ['async' => true,]);

        $mapping1 = [
            'source' => str_replace($bucketId, $linkedId, $table1Id),
            'destination' => 'languagesLoaded',
        ];

        $mapping2 = [
            'source' => str_replace($bucketId, $linkedId, $table2Id),
            'destination' => 'numbersLoaded',
        ];

        $mapping3 = [
            'source' => str_replace($bucketId, $linkedId, $table3Id),
            'destination' => 'numbersAliasLoaded',
        ];

        // init workspace
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $workspaceBackend,
            ],
            true,
        );

        $input = [$mapping1, $mapping2, $mapping3];

        // test if job is created and listed
        $initialJobs = $this->_client2->listJobs();

        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);

        $assertCallback = function ($events) use ($expectedLoadType) {
            $this->assertCount(3, $events);
            foreach ($events as $event) {
                self::assertSame($expectedLoadType, $event['results']['loadType']);
            }
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceLoaded')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client2, $assertCallback, $query);

        $afterJobs = $this->_client2->listJobs();

        self::assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        self::assertNotEquals(empty($initialJobs) ? 0 : $initialJobs[0]['id'], $afterJobs[0]['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client2->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        self::assertEquals(3, $export['totalCount']);
        self::assertCount(3, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        self::assertCount(3, $tables);
        self::assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        self::assertContains($backend->toIdentifier('numbersLoaded'), $tables);
        self::assertContains($backend->toIdentifier('numbersAliasLoaded'), $tables);

        // check table structure and data
        $data = $backend->fetchAll('languagesLoaded', \PDO::FETCH_ASSOC);
        self::assertCount(2, $data[0], 'there should be two columns');
        self::assertArrayHasKey('id', $data[0]);
        self::assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv((string) file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ',', '"'),
            $data,
            'id',
        );

        // now we'll load another table and use the preserve parameters to check that all tables are present
        // lets create it now to see if the table permissions are correctly propagated
        $table3Id = $this->_client->createTableAsync(
            $bucketId,
            'numbersLater',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv'),
        );

        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $mapping3 = ['source' => str_replace($bucketId, $linkedId, $table3Id), 'destination' => 'table3'];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3], 'preserve' => true]);

        $assertCallback = function ($events) use ($expectedLoadType) {
            self::assertCount(1, $events);
            self::assertSame($expectedLoadType, $events[0]['results']['loadType']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceLoaded')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client2, $assertCallback, $query);

        $tables = $backend->getTables();

        self::assertCount(4, $tables);
        self::assertContains($backend->toIdentifier('table3'), $tables);
        self::assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        self::assertContains($backend->toIdentifier('numbersLoaded'), $tables);
        self::assertContains($backend->toIdentifier('numbersAliasLoaded'), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3]]);

        $assertCallback = function ($events) use ($expectedLoadType) {
            self::assertCount(1, $events);
            self::assertSame($expectedLoadType, $events[0]['results']['loadType']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceLoaded')
            ->setTokenId($this->tokenId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client2, $assertCallback, $query);

        $tables = $backend->getTables();
        self::assertCount(1, $tables);
        self::assertContains($backend->toIdentifier('table3'), $tables);

        // unload validation
        $connection = $workspace['connection'];

        $backend = null; // force disconnect of same SNFLK connection
        $db = $this->getDbConnection($connection);

        if ($db instanceof \Doctrine\DBAL\Connection) {
            $db->executeQuery('CREATE TABLE [Languages3] (
			[Id] INTEGER NOT NULL,
			[NAME] VARCHAR(10) NOT NULL
		);');
            $db->executeQuery("INSERT INTO [Languages3] ([Id], [NAME]) VALUES (1, 'cz');");
            $db->executeQuery("INSERT INTO [Languages3] ([Id], [NAME]) VALUES (2, 'en');");
        } else {
            $db->query('CREATE TABLE "test.Languages3" (
			"Id" integer NOT NULL,
			"Name" varchar NOT NULL
		);');
            $db->query("INSERT INTO \"test.Languages3\" (\"Id\", \"Name\") VALUES (1, 'cz'), (2, 'en');");
        }
        try {
            $this->_client2->createTableAsyncDirect($linkedId, [
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'Languages3',
            ]);

            self::fail('Unload to liked bucket should fail with access exception');
        } catch (ClientException $e) {
            self::assertEquals('accessDenied', $e->getStringCode());
        }
    }
}
