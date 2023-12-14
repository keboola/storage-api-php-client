<?php



namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class CopyIntoWorkspaceTest extends WorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    public function testOverwrite(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace([], true);

        $client2 = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );

        $tableId = $this->createTableFromFile(
            $this->_client,
            $this->getTestBucketId(self::STAGE_IN),
            self::IMPORT_FILE_PATH,
        );

        $this->dropBucketIfExists($this->_client, 'out.c-linked-bucket', true);
        $this->dropBucketIfExists($client2, 'in.c-shared-bucket', true);

        $bucket = $client2->createBucket('shared-bucket', 'in');

        $this->createTableFromFile(
            $client2,
            $bucket,
            __DIR__ . '/../../_data/languages.more-rows.csv',
            'id',
            'languagesDetails2',
        );

        $client2->shareBucket($bucket);

        $sourceProjectId = $client2->verifyToken()['owner']['id'];
        $linkedBucketId = $this->_client->linkBucket(
            'linked-bucket',
            'out',
            $sourceProjectId,
            $bucket,
        );

        // first load
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                    ],
                ],
            ],
        );

        $backend = new SnowflakeWorkspaceBackend($workspace);
        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        // second load of same table with preserve
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tableId,
                            'destination' => 'Langs',
                        ],
                    ],
                    'preserve' => true,
                ],
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateTable', $e->getStringCode());
        }

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        try {
            // Invalid option combination
            $workspaces->loadWorkspaceData($workspace['id'], [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        'overwrite' => true,
                    ],
                ],
                'preserve' => false,
            ]);
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestLogicalException', $e->getStringCode());
        }

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        // third load table with more rows, preserve and overwrite
        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $linkedBucketId . '.languagesDetails2',
                    'destination' => 'Langs',
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ]);

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(6, $workspaceTableData);
    }

    /**
     * @param Client $client
     * @param string $bucketId
     * @param string $importFilePath
     * @param string|array $primaryKey
     * @param string $tableName
     * @return string
     */
    private function createTableFromFile(
        Client $client,
        $bucketId,
        $importFilePath,
        $primaryKey = 'id',
        $tableName = 'languagesDetails'
    ) {

        return $client->createTableAsync(
            $bucketId,
            $tableName,
            new CsvFile($importFilePath),
            ['primaryKey' => $primaryKey],
        );
    }
}
