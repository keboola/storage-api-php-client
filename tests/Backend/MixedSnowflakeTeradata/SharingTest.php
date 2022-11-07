<?php

namespace Keboola\Test\Backend\MixedSnowflakeTeradata;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
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
     * @return array[]
     */
    public function sharingBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE],
            [self::BACKEND_TERADATA],
        ];
    }

    /**
     * @return array[]
     */
    public function workspaceMixedBackendData()
    {
        return [
//            'td to snowflake' =>
//                [
//                    'sharing backend' => self::BACKEND_SNOWFLAKE,
//                    'workspace backend' => self::BACKEND_TERADATA,
//                    'load type' => 'staging',
//                ],
            'td to td' =>
                [
                    'sharing backend' => self::BACKEND_TERADATA,
                    'workspace backend' => self::BACKEND_TERADATA,
                    'load type' => 'direct',
                ],
        ];
    }

    /**
     * @return void
     */
    public function testOrganizationAdminInTokenVerify(): void
    {
        $token = $this->_client->verifyToken();
        self::assertTrue($token['admin']['isOrganizationMember']);
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
    ): void
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets($sharingBackend);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $bucketDetail = $this->_client->getBucket($bucketId);
        $secondBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languagesFromClient1',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            []
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
            $sharedBucket['id']
        );
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $workspaceBackend,
            ],
            true
        );

        $wsConnection = $workspace['connection'];
        $wsDb = $this->getDbConnection($wsConnection);
        self::assertInstanceOf(\Doctrine\DBAL\Connection::class, $wsDb);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $sharedBucketDbNameSuffix = $sharedBucket['project']['id'] . '-' . str_replace('.', '_', $sharedBucket['id']);


        $data = $wsDb->fetchAllAssociative(sprintf('SELECT * FROM DBC.DatabasesVX WHERE DatabaseName LIKE %s', TeradataQuote::quote('%' . $sharedBucketDbNameSuffix)));
        $this->assertCount(1, $data);
        $sourceBucketDBName = $data[0]['DatabaseName'];
        $wsBackend->createTable('tableInWs', ['NAME' => 'VARCHAR']);
        $wsBackend->executeQuery("INSERT INTO `tableInWs` ('pat', 'mat')");

        // check that the tables are in the workspace
        $dataClient1 = $wsDb->fetchAllAssociative(sprintf('SELECT * FROM %s.%s', TeradataQuote::quoteSingleIdentifier($sourceBucketDBName), TeradataQuote::quoteSingleIdentifier('languagesFromClient1')));
        $dataClient2 = $wsBackend->fetchAll('tableInWs');

        $a = '1';
    }
}
