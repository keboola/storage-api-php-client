<?php
/**
 *
 * User: Erik Zigo
 *
 */
namespace Keboola\Test\Backend\Sharing;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class BucketsTest extends StorageApiTestCase
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

        $this->_initEmptyTestBuckets();
    }

    /**
     * Init empty bucket test helper
     * @param $name
     * @param $stage
     * @return bool|string
     */
    private function initEmptyBucket($name, $stage)
    {
        try {
            $bucket = $this->_client->getBucket("$stage.c-$name");
            $tables = $this->_client->listTables($bucket['id']);
            foreach ($tables as $table) {
                $this->_client->dropTable($table['id']);
            }

            return $bucket['id'];
        } catch (\Keboola\StorageApi\ClientException $e) {
            return $this->_client->createBucket($name, $stage, 'Api tests');
        }
    }

    protected function _initEmptyTestBuckets()
    {
        // unlink buckets
        foreach ($this->_client2->listBuckets() as $bucket) {
            if (!empty($bucket['sourceBucket'])) {
                $this->_client2->dropBucket($bucket['id']);
            }
        }

        // unshare buckets
        foreach ($this->_client->listBuckets() as $bucket) {
            if ($this->_client->isSharedBucket($bucket['id'])) {
                $this->_client->unshareBucket($bucket['id']);
            }
        }

        // init empty buckets
        foreach (array(self::STAGE_OUT, self::STAGE_IN) as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-sharing-tests', $stage);
        }
    }

    public function testShareBucket()
    {
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

    public function testSharedBuckets()
    {
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

    public function testLinkBucket()
    {
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

    private function validateTablesMetadata($sharedBucketId, $linkedBucketId)
    {
        $fieldNames = [
            'name', 'columns', /*'isAlias',*/
            'primaryKey', 'indexedColumns',
            'name', 'dataSizeBytes', 'rowsCount',
            /*'lastChangeDate',*/ 'lastImportDate',
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

            $this->assertEquals($data, $linkedData);
        }
    }

    public function testLinkedBucket()
    {
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../_data/pk.simple.csv'),
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
            new CsvFile(__DIR__ . '/../_data/pk.simple.increment.csv'),
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
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'second',
            new CsvFile(__DIR__ . '/../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        //@FIXME lastChangeDate validation problem (different seconds)
    }

    public function testRestrictedDrop()
    {
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../_data/pk.simple.csv'),
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
            //    $this->assertEquals('tables.cannotDeleteTableWithLinks', $e->getStringCode());
            }

            //@FIXME
            /*
            try {
                $this->_client->deleteTableColumn($table['id'], 'name');
                $this->fail('Shared table column delete should fail');
            } catch (ClientException $e) {
                $this->assertEquals('tables.cannotDeleteRowWithLinks', $e->getStringCode());
                $this->assertEquals('tables.cannotDeleteRowWithLinks', $e->getStringCode());
            }
            */
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
}