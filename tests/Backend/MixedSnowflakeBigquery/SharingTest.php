<?php

namespace Keboola\Test\Backend\MixedSnowflakeBigquery;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\Test\Backend\Mixed\StorageApiSharingTestCase;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SharingTest extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;

    /**
     * @dataProvider sharingMethodProvider
     * @throws ClientException
     * @throws \Doctrine\DBAL\Exception
     * @throws \Keboola\StorageApi\Exception
     */
    public function testAccessDataInLinkedBucketFromWSViaRO(string $shareMethod, ?bool $async): void
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets(self::BACKEND_BIGQUERY);
        $bucketId = $this->getTestBucketId();

        // create table in SHARING bucket in project A
        $this->_client->createTableAsync(
            $bucketId,
            'languagesFromClient1',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
        );

        // share and link bucket A->B
        switch ($shareMethod) {
            case 'shareBucketToProjects':
                $targetProjectId = $this->_client2->verifyToken()['owner']['id'];
                $this->_client->shareBucketToProjects($bucketId, [$targetProjectId], $async ?? false);
                break;
            case 'shareBucketToUsers':
                $targetUser = $this->_client2->verifyToken()['admin'];
                $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']], $async ?? false);
                break;
            default:
                // deprecated method shareBucket use as parameter array of options
                // async and sync action use the same service at background so in case of this deprecated method
                // is good enough to test only one time
                $this->_client->$shareMethod($bucketId, $async ?? []);
        }

        self::assertTrue($this->_client->isSharedBucket($bucketId));
        $response = $this->_client2->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $this->_client2->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // create WS in project B
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace(
            ['backend' => self::BACKEND_BIGQUERY],
            true
        );

        /** @var BigQueryClient $wsDb */
        $wsDb = $this->getDbConnection($workspace['connection']);
        self::assertInstanceOf(BigQueryClient::class, $wsDb);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // create table in WS and insert some data
        $wsBackend->createTable('tableInWs', ['NAME' => 'STRING']);
        $wsBackend->executeQuery(sprintf(
            "INSERT INTO %s.%s VALUES ('pat')",
            BigqueryQuote::quoteSingleIdentifier($workspace['name']),
            BigqueryQuote::quoteSingleIdentifier('tableInWs')
        ));
        $wsBackend->executeQuery(sprintf(
            "INSERT INTO %s.%s VALUES ('mat')",
            BigqueryQuote::quoteSingleIdentifier($workspace['name']),
            BigqueryQuote::quoteSingleIdentifier('tableInWs')
        ));

        $dataset = $wsDb->datasets();
        // check there is two datasets available in workspace one is linked bucket
        // second is workspace self
        $this->assertCount(2, $dataset);

        // check is exist manually created table in workspaces
        $wsDataset = $wsDb->dataset($workspace['name']);
        $this->assertTrue($wsDataset->exists());
        $tableCreatedInWs = $wsDataset->table('tableInWs');
        $this->assertTrue($tableCreatedInWs->exists());

        // check can select from linked table
        $linkedBucketSchemaName = 'out_c_' . str_replace('-', '_', $linkedBucketName);
        $linkedDataset = $wsDb->dataset($linkedBucketSchemaName);
        $this->assertTrue($linkedDataset->exists());
        $tableInLinkedDataset = $linkedDataset->table('languagesFromClient1');
        $this->assertTrue($tableInLinkedDataset->exists());

        // check numbers of rows of table manually created in WS
        // and linked bucket
        $this->assertCount(2, $tableCreatedInWs->rows());
        $this->assertCount(5, $tableInLinkedDataset->rows());
    }

    public function sharingMethodProvider(): Generator
    {
        yield 'shareBucket' => [
            'shareBucket',
            null,
        ];

        foreach ([true, false] as $async) {
            yield sprintf('shareOrganizationBucket, async=%s', $async) => [
                'shareOrganizationBucket',
                $async,
            ];

            yield sprintf('shareOrganizationProjectBucket, async=%s', $async) => [
                'shareOrganizationProjectBucket',
                $async,
            ];

            yield  sprintf('shareBucketToProjects, async=%s', $async) => [
                'shareBucketToProjects',
                $async,
            ];

            yield  sprintf('shareBucketToUsers, async=%s', $async) => [
                'shareBucketToUsers',
                $async,
            ];
        }
    }

    public function testAllSharingOperations(): void
    {
        //setup test tables
        $this->initTestBuckets(self::BACKEND_BIGQUERY);
        $bucketId = $this->getTestBucketId();

        // share and link bucket A->B
        $this->_client->shareBucket($bucketId);
        self::assertTrue($this->_client->isSharedBucket($bucketId));
        $response = $this->_client2->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        // link
        /** @var string $linkedId */
        $linkedId = $this->_client2->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // unlink
        $this->_client2->dropBucket($linkedId, ['async' => true]);
        self::assertEmpty($this->_client2->listBuckets(['include' => 'linkedBuckets']));

        // unshare
        $this->_client->unshareBucket($bucketId);
        self::assertFalse($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        self::assertCount(0, $response);

        // share back
        $this->_client->shareBucket($bucketId);
        self::assertTrue($this->_client->isSharedBucket($bucketId));

        // link again!
        $this->_client2->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        $linkedBuckets = $this->_client2->listBuckets(['include' => 'linkedBuckets']);
        $this->assertCount(1, $linkedBuckets);
        /** @var array $firstLinked */
        $firstLinked = reset($linkedBuckets);
        $this->assertEquals($bucketId, $firstLinked['sourceBucket']['id']);
    }

    public function testCreateTableInBucketWithDifferentBackendAsProjectDefault(): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = $this->getTestBucketId();

        // create table in SHARING bucket in project A
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languagesFromClient1',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
        );

        $preview = $this->_client->getTableDataPreview($tableId);
        $this->assertCount(5, Client::parseCsv($preview));
    }
}
