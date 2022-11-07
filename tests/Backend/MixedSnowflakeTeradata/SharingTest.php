<?php

namespace Keboola\Test\Backend\MixedSnowflakeTeradata;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\Test\Backend\Mixed\StorageApiSharingTestCase;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SharingTest extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;


    /**
     * @return void
     */
    public function testOrganizationAdminInTokenVerify(): void
    {
        $token = $this->_client->verifyToken();
        self::assertTrue($token['admin']['isOrganizationMember']);
    }

    /**
     * @return void
     * @throws ClientException
     * @throws \Doctrine\DBAL\Exception
     * @throws \Keboola\StorageApi\Exception
     */
    public function testAccessDataInLinkedBucketFromWSViaRO(): void
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets(self::BACKEND_TERADATA);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        // create table in SHARING bucket in project A
        $this->_client->createTableAsync(
            $bucketId,
            'languagesFromClient1',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
        );

        // share and link bucket A->B
        $this->_client->shareBucket($bucketId);
        self::assertTrue($this->_client->isSharedBucket($bucketId));
        $response = $this->_client2->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $this->_client2->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // create WS in project B
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace(
            ['backend' => self::BACKEND_TERADATA],
            true
        );

        $wsDb = $this->getDbConnection($workspace['connection']);
        self::assertInstanceOf(\Doctrine\DBAL\Connection::class, $wsDb);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // create table in WS and insert some data
        $wsBackend->createTable('tableInWs', ['NAME' => 'VARCHAR']);
        $wsBackend->executeQuery(sprintf("INSERT INTO %s VALUES ('pat')", TeradataQuote::quoteSingleIdentifier('tableInWs')));
        $wsBackend->executeQuery(sprintf("INSERT INTO %s VALUES ('mat')", TeradataQuote::quoteSingleIdentifier('tableInWs')));

        // try to guess name of TD DB of bucket1 in projectA. The structure of DB is <stackPrefix><projectID>-<stage>_<bucketID>
        $sharedBucketDbNameSuffix = sprintf(
            '%%%s-%s',
            $sharedBucket['project']['id'],
            str_replace('.', '_', $sharedBucket['id'])
        );

        $foundDatabases = $wsDb->fetchAllAssociative(
            sprintf(
                'SELECT * FROM DBC.DatabasesVX WHERE DatabaseName LIKE %s',
                TeradataQuote::quote($sharedBucketDbNameSuffix)
            )
        );
        $this->assertCount(1, $foundDatabases);
        $sourceBucketDBName = $foundDatabases[0]['DatabaseName'];
        assert(is_string($sourceBucketDBName));

        // check that both tables are available in the workspace
        $dataFromLinkedTable = $wsDb->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($sourceBucketDBName),
                TeradataQuote::quoteSingleIdentifier('languagesFromClient1')
            )
        );
        $dataCreatedInWs = $wsBackend->fetchAll('tableInWs');

        $this->assertCount(2, $dataCreatedInWs);
        $this->assertCount(5, $dataFromLinkedTable);
    }
}
