<?php
/**
 *
 * User: Erik Zigo
 *
 */
namespace Keboola\Test\Backend\Mixed;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;

class SharingTest extends StorageApiTestCase
{
    /**
     * @var Client
     */
    private $_client2;

    public function setUp()
    {
        parent::setUp();

        $this->_client2 = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ));
    }

    /**
     * Remove all workspaces in both projects
     */
    private function deleteAllWorkspaces()
    {
        /**
         * @var Client[] $clients
         */
        $clients = [
            $this->_client,
            $this->_client2,
        ];

        // unlink buckets
        foreach ($clients as $client) {
            $workspaces = new Workspaces($client);
            foreach ($workspaces->listWorkspaces() as $workspace) {
                $workspaces->deleteWorkspace($workspace['id']);
            }
        }
    }

    /**
     * Init empty bucket test helper
     *
     * @param $name
     * @param $stage
     * @return bool|string
     */
    private function initEmptyBucket($name, $stage, $backend)
    {
        if ($this->_client->bucketExists("$stage.c-$name")) {
            $this->_client->dropBucket(
                "$stage.c-$name",
                [
                    'force' => true,
                ]
            );
        }

        return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
    }

    /**
     * Unlinks and unshare all buckets from both projects
     * Then recreates test buckets in given backend
     *
     * @param $backend
     * @return array created bucket ids
     * @throws ClientException
     */
    private function initTestBuckets($backend)
    {
        /**
         * @var Client[] $clients
         */
        $clients = [
            $this->_client,
            $this->_client2,
        ];

        // unlink buckets
        foreach ($clients as $client) {
            foreach ($client->listBuckets() as $bucket) {
                if (!empty($bucket['sourceBucket'])) {
                    $client->dropBucket($bucket['id']);
                }
            }
        }

        // unshare buckets
        foreach ($clients as $client) {
            foreach ($client->listBuckets() as $bucket) {
                if ($client->isSharedBucket($bucket['id'])) {
                    $client->unshareBucket($bucket['id']);
                }
            }
        }

        // recreate buckets in firs project
        $this->_bucketIds = [];
        foreach (array(self::STAGE_OUT, self::STAGE_IN) as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-sharing', $stage, $backend);
        }

        return $this->_bucketIds;
    }

    public function testOrganizationAdminInTokenVerify()
    {
        $token = $this->_client->verifyToken();
        $this->assertTrue($token['admin']['isOrganizationMember']);
    }

    public function testInvalidSharingType()
    {
        $this->initTestBuckets(self::BACKEND_MYSQL);
        $bucketId = reset($this->_bucketIds);

        try {
            $this->_client->shareBucket($bucketId, [
                'sharing' => 'global',
            ]);
            $this->fail('Bucket should not be shared');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.invalidSharingType', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    public function testOrganizationPublicSharing()
    {
        $this->initTestBuckets(self::BACKEND_MYSQL);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId, [
            'sharing' => 'organization-project',
        ]);

        $tokenId = $this->_client2->createToken('manage', 'Test Token', 3600);
        $token = $this->_client2->getToken($tokenId);
        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $client->verifyToken();

        // bucket can be listed with non-admin sapi token
        $sharedBuckets = $client->listSharedBuckets();
        $this->assertCount(1, $sharedBuckets);

        $this->assertEquals($bucketId, $sharedBuckets[0]['id']);
        $this->assertEquals('organization-project', $sharedBuckets[0]['sharing']);

        // bucket can be linked
        $linkedBucketId = $client->linkBucket(
            'organization-project-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id']
        );

        $bucket = $client->getBucket($linkedBucketId);
        $this->assertEquals($sharedBuckets[0]['id'], $bucket['sourceBucket']['id']);
        $this->assertEquals($sharedBuckets[0]['project']['id'], $bucket['sourceBucket']['project']['id']);
    }

    public function testNonOrganizationAdminInToken()
    {
        $this->initTestBuckets(self::BACKEND_MYSQL);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);
        $linkedId = $this->_client->linkBucket(
            "linked-" . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // new token creation
        $tokenId = $this->_client->createToken('manage', 'Test Token', 3600);
        $token = $this->_client->getToken($tokenId);

        $client = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ));

        $client->verifyToken();

        $this->assertEmpty($client->listSharedBuckets());

        try {
            $client->shareBucket($bucketId);
            $this->fail('`shareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->unshareBucket($bucketId);
            $this->fail('`unshareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->linkBucket(
                "linked-" . time(),
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id']
            );
            $this->fail('`linkBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->dropBucket($linkedId);
            $this->fail('`dropBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucket($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $this->_client->unshareBucket($bucketId);
        $this->assertFalse($this->_client->isSharedBucket($bucketId));

        // sharing twice
        $this->_client->shareBucket($bucketId);

        try {
            $this->_client->shareBucket($bucketId);
            $this->fail("sharing twice should fail");
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.shareTwice', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testSharedBuckets($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $response);

        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);

        $project = $response['owner'];

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        foreach ($response as $sharedBucket) {
            $this->assertArrayHasKey('id', $sharedBucket);
            $this->assertArrayHasKey('description', $sharedBucket);
            $this->assertArrayHasKey('project', $sharedBucket);

            $this->assertArrayHasKey('id', $sharedBucket['project']);
            $this->assertArrayHasKey('name', $sharedBucket['project']);

            $this->assertEquals($sharedBucket['project']['id'], $project['id']);
            $this->assertEquals($sharedBucket['project']['name'], $project['name']);
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkBucket($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $sourceBucket = $this->_client->getBucket($bucketId);

        $this->_client->shareBucket($bucketId);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket("linked-" . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);

        $bucket = $this->_client2->getBucket($id);

        $this->assertArrayHasKey('id', $bucket);
        $this->assertArrayHasKey('stage', $bucket);
        $this->assertArrayHasKey('backend', $bucket);
        $this->assertArrayHasKey('description', $bucket);
        $this->assertArrayHasKey('isReadOnly', $bucket);

        $this->assertEquals($id, $bucket['id']);
        $this->assertEquals('out', $bucket['stage']);
        $this->assertTrue($bucket['isReadOnly']);
        $this->assertEquals($sourceBucket['backend'], $bucket['backend']);
        $this->assertEquals($sourceBucket['description'], $bucket['description']);
    }

    public function testBucketCannotBeLinkedMoreTimes()
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);

        $response = $this->_client2->listSharedBuckets();
        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket("linked-" . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);
        try {
            $this->_client2->linkBucket("linked-" . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);
            $this->fail('bucket should not be linked');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }
    }

    private function validateTablesMetadata($sharedBucketId, $linkedBucketId)
    {
        $fieldNames = [
            'name', 'columns',
            'primaryKey', 'indexedColumns',
            'name', 'dataSizeBytes', 'rowsCount',
            'lastImportDate',
        ];

        $tables = $this->_client->listTables($sharedBucketId, ['include' => 'columns']);
        $linkedTables = $this->_client2->listTables($linkedBucketId, ['include' => 'columns']);

        foreach ($tables as $i => $table) {
            foreach ($fieldNames as $fieldName) {
                $this->assertEquals(
                    $table[$fieldName],
                    $linkedTables[$i][$fieldName],
                    sprintf("Bad value for `%s` metadata attribute", $fieldName)
                );
            }

            $data = $this->_client->exportTable($table['id']);
            $linkedData = $this->_client2->exportTable($linkedTables[$i]['id']);

            $this->assertLinesEqualsSorted($data, $linkedData);
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkedBucket($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $this->_client->shareBucket($bucketId);

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            "linked-" . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );


        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        $this->validateTablesMetadata($bucketId, $linkedBucketId);


        // new import
        $this->_client->writeTable(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
            [
                'primaryKey' => 'id',
                'incremental' => true,
            ]
        );

        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // new index
        $this->_client->markTableColumnAsIndexed($tableId, 'name');
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // drop index
        $this->_client->removeTableColumnFromIndexed($tableId, 'name');
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // remove primary key
        $this->_client->removeTablePrimaryKey($tableId);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // add primary key
        $this->_client->createTablePrimaryKey($tableId, ['id', 'name']);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // add column
        $this->_client->addTableColumn($tableId, 'fake');
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // delete rows
        $this->_client->deleteTableRows($tableId, [
            'whereColumn' => 'id',
            'whereValues' => ['new'],
        ]);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // aditional table
        $this->_client->createTableAsync(
            $bucketId,
            'second',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );
        $this->validateTablesMetadata($bucketId, $linkedBucketId);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testRestrictedDrop($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $this->_client->shareBucket($bucketId);

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            "linked-" . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(1, $tables);

        // table drop
        foreach ($this->_client->listTables($bucketId) as $table) {
            try {
                $this->_client->dropTable($table['id']);
                $this->fail('Shared table delete should fail');
            } catch (ClientException $e) {
                $this->assertEquals('tables.cannotDeletedTableWithAliases', $e->getStringCode());
            }

            try {
                $this->_client->deleteTableColumn($table['id'], 'name');
                $this->fail('Shared table column delete should fail');
            } catch (ClientException $e) {
                $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
            }
        }

        // bucket drop
        try {
            $this->_client->dropBucket($bucketId);
            $this->fail('Shared bucket delete should fail');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }

        $this->validateTablesMetadata($bucketId, $linkedBucketId);
    }

    /**
     *
     *
     * @dataProvider workspaceMixedBackendData
     * @throws ClientException
     * @throws \Exception
     * @throws \Keboola\StorageApi\Exception
     */
    public function testWorkspaceLoadData($sharingBackend, $workspaceBackend)
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets($sharingBackend);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $secondBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $table1Id = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2Id = $this->_client->createTable(
            $bucketId,
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

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

        // init workspace
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace([
            "backend" => $workspaceBackend
        ]);

        $input = array($mapping1, $mapping2);

        // test if job is created and listed
        $initialJobs = $this->_client2->listJobs();
        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
        $afterJobs = $this->_client2->listJobs();


        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals(empty($initialJobs) ? 0 : $initialJobs[0]['id'], $afterJobs[0]['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client2->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(2, $export['totalCount']);
        $this->assertCount(2, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(2, $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);

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

        // now we'll load another table and use the preserve parameters to check that all tables are present
        // lets create it now to see if the table permissions are correctly propagated
        $table3Id = $this->_client->createTable(
            $bucketId,
            'numbersLater',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $mapping3 = array("source" => str_replace($bucketId, $linkedId, $table3Id), "destination" => "table3");
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3), "preserve" => true));

        $tables = $backend->getTables();

        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);


        // unload validation
        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        try {
            $this->_client2->createTableAsyncDirect($linkedId, array(
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages3',
            ));

            $this->fail('Unload to liked bucket should fail with access exception');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @param $connection
     * @return Connection|\PDO
     * @throws \Exception
     */
    protected function getDbConnection($connection)
    {
        if ($connection['backend'] === parent::BACKEND_SNOWFLAKE) {
            $db = new Connection([
                'host' => $connection['host'],
                'database' => $connection['database'],
                'warehouse' => $connection['warehouse'],
                'user' => $connection['user'],
                'password' => $connection['password'],
            ]);
            // set connection to use workspace schema
            $db->query(sprintf("USE SCHEMA %s;", $db->quoteIdentifier($connection['schema'])));

            return $db;
        } else if ($connection['backend'] === parent::BACKEND_REDSHIFT) {
            $pdo = new \PDO(
                "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
                $connection['user'],
                $connection['password']
            );
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            return $pdo;
        } else {
            throw new \Exception("Unsupported Backend for workspaces");
        }
    }


    public function sharingBackendData()
    {
        return [
            [self::BACKEND_MYSQL],
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_MYSQL, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_MYSQL, self::BACKEND_REDSHIFT],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT],
        ];
    }
}
